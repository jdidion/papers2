# Wrapper around a pyzotero session that can convert Papers2
# entities to zotero items.
#
# TODO: item types: manuscript report thesis
# TODO: handle archived papers?
# TODO: user-definable date format; for now using YYYY-MM-DD
# TODO: use relations to link book chapters to parent volume

from datetime import datetime
import logging as log
import sys

from pyzotero.zotero import Zotero
from .schema import PubType, IDSource, KeywordType, Label
from .util import Batch, JSONWriter

# mapping of papers2 publication types 
# to Zotero item types 
ITEM_TYPES = {
    PubType.BOOK                : 'book',
    PubType.THESIS              : 'thesis',
    PubType.E_BOOK              : 'book',
    #PubType.BOOK_SECTION       : 'bookSection',
    PubType.WEBSITE             : 'webpage',
    PubType.SOFTWARE            : 'computerProgram',
    PubType.JOURNAL_ARTICLE     : 'journalArticle',
    PubType.NEWSPAPER_ARTICLE   : 'newspaperArticle',
    PubType.WEBSITE_ARTICLE     : 'webpage',
    PubType.PREPRINT            : 'journalArticle',
    PubType.CONFERENCE_PAPER    : 'conferencePaper',
    PubType.REPORT              : 'report',
    PubType.PROTOCOL            : 'report'
}

class Extract(object):
    def __init__(self, fn=None, num_values=1):
        self.fn = fn
        self.num_values = num_values
    
    def extract(self, pub, context, default=None):
        if self.fn is not None:
            value = self.fn(pub)
        
        else:
            try:
                value = self.get_value(pub, context)
            
            except NotImplementedError:
                value = default
        
        if value is not None:
            if isinstance(value, str) or isinstance(value, unicode):
                value = self.format(value)

            else:
                try:
                    value = tuple(value)
                    nvals = len(value)
                    return_tuple = self.num_values != 1
                    if self.num_values is not None:
                        nvals = min(nvals, self.num_values)
                    value = self.format_tuple(value, nvals)
                    if value is not None:
                        if len(value) == 0:
                            value = None
                        elif nvals == 1 and not return_tuple:
                            value = value[0]
                
                except TypeError:
                    value = self.format(value)
        
        return value
    
    def format_tuple(self, values, nvals):
        values = filter(None, values)
        nvals = min(nvals, len(values))
        if nvals > 0:
            if len(values) < nvals:
                values = values[0:nvals]
            return map(self.format, values)
    
    def format(self, value):
        return value
    
    def get_value(self, pub, context):
        raise NotImplementedError()

class ExtractRange(Extract):
    def format_tuple(self, values, nvals):
        return ("{0}-{1}".format(*values),)

class ExtractTimestamp(Extract):
    def format(self, value):
        datetime.fromtimestamp(value)

class ExtractBundle(Extract):
    def get_value(self, pub, context):
        journal = context.papers2.get_bundle(pub)
        if journal is not None:
            return journal.title
        else:
            return pub.bundle_string

class ExtractPubdate(Extract):
    def format(self, pub_date):
        date_str = ''
        
        year = pub_date[2:6]
        if year is not None:
            date_str = year
            
            month = pub_date[6:8]
            if month is not None:
                if month == "00":
                    month = "01"
                date_str += "-" + month
                
                day = pub_date[8:10]
                if day is not None:
                    if day == "00":
                        day = "01"
                    date_str += "-" + day
        
        # TODO: check date for validity
        
        return date_str

class ExtractCreators(Extract):
    def __init__(self):
        Extract.__init__(self, num_values=None)
    
    def get_value(self, pub, context):
        return context.papers2.get_pub_authors(pub)
    
    def format(self, author):
        if author.type == 0:
            creator_type = u'author'
        elif author.type == 1:
            creator_type = u'editor'
        else:
            raise Exception("Unsupported author type {0}".format(author.type))
        
        if author.institutional > 0:
            return { 
                u'creatorType': creator_type,
                u'name': author.surname
            }
        else:
            return { 
                u'creatorType': creator_type,
                u'firstName': author.prename,
                u'lastName': author.surname
            }

class ExtractIdentifier(Extract):
    def __init__(self, id_sources, num_values=1):
        Extract.__init__(self, num_values=num_values)
        self.id_sources = id_sources
        
    def get_value(self, pub, context):
        idents = []
        for src in self.id_sources:
            idents.extend(context.papers2.get_identifiers(pub, src))
        return idents
    
    def format_value(self, value):
        return value.remote_id

class ExtractPubmedID(ExtractIdentifier):
    def __init__(self):
        ExtractIdentifier.__init__(self, (IDSource.PUBMED, IDSource.PMC))
    
    def format(self, value):
        return "PMID: {0}".format(value.remote_id) 

class ExtractUrl(Extract):
    def get_value(self, pub, context):
        return context.papers2.get_urls(pub)
        
    def format(self, value):
        return value.remote_id

class ExtractKeywords(Extract):
    def __init__(self):
        Extract.__init__(self, num_values=None)
        
    def get_value(self, pub, context):
        keywords = []
        if 'user' in context.keyword_types:
            keywords.extend(k.name for k in context.papers2.get_keywords(pub, KeywordType.USER))
        if 'auto' in context.keyword_types:
            keywords.extend(k.name for k in context.papers2.get_keywords(pub, KeywordType.AUTO))
        if 'label' in context.keyword_types:
            label = context.label_map.get(context.papers2.get_label_name(pub), None)
            if label is not None:
                keywords.append(label)
        return keywords

class ExtractCollections(Extract):
    def __init__(self):
        Extract.__init__(self, num_values=None)
    
    def get_value(self, pub, context):
        if len(context.collections) > 0:
            collections = []
            for c in context.papers2.get_collections(pub):
                if c.name in context.collections:
                    collections.append(context.collections[c.name])
            return collections
                
class AttrExtract(Extract):
    def __init__(self, key):
        self.key = key
    
    def get_value(self, pub, context):
        return getattr(pub, self.key)

EXTRACTORS = dict(
    DOI=                    Extract(lambda pub: pub.doi),
    ISBN=                   ExtractIdentifier((IDSource.ISBN, IDSource.ISSN)),
    abstractNote=           Extract(lambda pub: pub.summary),
    accessDate=             ExtractTimestamp(lambda pub: pub.imported_date),
    collections=            ExtractCollections(),
    creators=               ExtractCreators(),
    date=                   ExtractPubdate(lambda pub: pub.publication_date),
    edition=                Extract(lambda pub: pub.version),
    extra=                  ExtractPubmedID(),
    issue=                  Extract(lambda pub: pub.number),
    journalAbbreviation=    Extract(lambda pub: pub.bundle_string),
    language=               Extract(lambda pub: pub.language),
    number=                 Extract(lambda pub: pub.document_number),
    pages=                  ExtractRange(lambda pub: (pub.startpage, pub.endpage)),
    numPages=               Extract(lambda pub: pub.startpage),
    place=                  Extract(lambda pub: pub.place),
    publicationTitle=       ExtractBundle(),
    publisher=              Extract(lambda pub: pub.publisher),
    rights=                 Extract(lambda pub: pub.copyright),
    tags=                   ExtractKeywords(),
    title=                  Extract(lambda pub: pub.title),
    university=             ExtractBundle(),
    url=                    ExtractUrl(),
    volume=                 Extract(lambda pub: pub.volume)
)

class ZoteroImporter(object):
    def __init__(self, library_id, library_type, api_key, papers2,
            keyword_types=('user','label'), label_map={}, add_to_collections=[], 
            upload_attachments="all", batch_size=50, checkpoint=None, dryrun=None):
        self.client = Zotero(library_id, library_type, api_key)
        self.papers2 = papers2
        self.keyword_types = keyword_types
        self.label_map = label_map
        self.upload_attachments = upload_attachments
        self.checkpoint = checkpoint
        self.dryrun = JSONWriter(dryrun) if dryrun is not None else None
        self._batch = Batch(batch_size)
        self._load_collections(add_to_collections)
    
    # Load Zotero collections and create any
    # Papers2 collections that don't exist.
    # TODO: need to handle collection hierarchies
    def _load_collections(self, add_to_collections):
        self.collections = {}
        if add_to_collections is None:
            add_to_collections = list(c.name for c in self.papers2.get_collections())

        if len(add_to_collections) > 0:
            if self.dryrun is not None:
                for c in add_to_collections:
                    self.collections[c] = "<{0}>".format(c)
                
            else:
                # fetch existing zotero collections
                existing_collections = {}
                for zc in self.client.collections():
                    data = zc['data']
                    existing_collections[data['name']] = data['key']
                
                # add any papers2 collections that do not already exist
                payload = []
                for pc in add_to_collections:
                    if pc not in existing_collections:
                        payload.append(dict(name=pc))
                if len(payload) > 0:
                    self.client.create_collection(payload)
            
                # re-fetch zotero collections in order to get keys
                for zc in self.client.collections():
                    data = zc['data']
                    if data['name'] in add_to_collections:
                        self.collections[data['name']] = data['key']
    
    def add_pub(self, pub):
        # ignore publications we've already imported
        if self.checkpoint is not None and self.checkpoint.contains(pub.ROWID):
            log.debug("Skipping already imported publication {0}".format(pub.ROWID))
            return False
        
        # convert the Papers2 publication type to a Zotero item type
        item_type = ITEM_TYPES[self.papers2.get_pub_type(pub)]
        
        # get the template to fill in for an item of this type
        item = self.client.item_template(item_type)

        # fill in template fields
        for key, value in item.iteritems():
            if key in EXTRACTORS:
                value = EXTRACTORS[key].extract(pub, self, value)
                if value is not None:
                    item[key] = value

        # add notes, if any
        notes = []
        if pub.notes is not None and len(pub.notes) > 0:
            notes.append(pub.notes)
        
        reviews = self.papers2.get_reviews(pub)
        for r in reviews:
            notes.append("{0} Rating: {1}".format(r.content, r.rating))
        
        # get paths to attachments
        attachments = []
        if self.upload_attachments == "all" or (
                self.upload_attachments == "unread" and pub.times_read == 0):
            attachments = list(self.papers2.get_attachments(pub))
        
        # add to batch and checkpoint
        self._batch.add(item, notes, attachments)
        if self.checkpoint is not None:
            self.checkpoint.add(pub.ROWID)
        
        # commit the batch if it's full
        self._commit_batch()
        
        return True
    
    def close(self):
        if self._batch is not None:
            self._commit_batch(force=True)
            self._batch = None
        if self.dryrun is not None:
            self.dryrun.close()
            
    def _commit_batch(self, force=False):
        if self._batch.is_full or (force and not self._batch.is_empty):
            try:
                if self.dryrun is not None:
                    for item, attachments in self._batch.iter():
                        self.dryrun.write(item, attachments)
                
                else:
                    # upload metadata
                    status = self.client.create_items(self._batch.items)
                    
                    if len(status['failed']) > 0:
                        for status_idx, status_msg in status['failed'].iteritems():
                            item_idx = int(status_idx)
                            # remove failures from the checkpoint
                            if self.checkpoint is not None:
                                self.checkpoint.remove(item_idx)
                            item = self._batch.items[item_idx]
                            log.error("Upload failed for item {0}; code {1}; {2}".format(
                               item['title'], status_msg['code'], status_msg['message']))
                
                    successes = {}
                    successes.update(status['success'])
                    successes.update(status['unchanged'])
                    
                    for k, objKey in successes.iteritems():
                        item_idx = int(k)
                        
                        # add notes
                        notes = self._batch.notes[item_idx]
                        if len(notes) > 0:
                            note_batch = []
                            for note_text in notes:
                                note = self.client.item_template('note')
                                note['parentItem'] = objKey
                                note['note'] = note_text
                                note_batch.append(note)
                            
                            note_status = self.client.create_items(note_batch)
                            
                            if len(note_status['failed']) > 0:
                                for status_idx, status_msg in note_status['failed'].iteritems():
                                    note_idx = int(status_idx)
                                    # just warn about these failures
                                    note = note_batch[note_idx]
                                    log.error("Failed to create note {0} for item item {1}; code {2}; {3}".format(
                                       note['note'], self.batch.items[idx]['title'], 
                                       status_msg['code'], status_msg['message']))
                    
                        # upload attachments and add items to collections
                        if self.upload_attachments != "none":
                        
                            # TODO: modify pyzotero to pass MIME type for contentType key
                            attachments = list(path for path, mime in self._batch.attachments[item_idx])
                            if len(attachments) > 0:
                                try:
                                    self.client.attachment_simple(attachments, objKey)

                                # This is to work around a bug in pyzotero where an exception is
                                # thrown if an attachment already exists
                                except KeyError:
                                    log.info("One or more attachment already exists: {0}".format(",".join(attachments)))
                
                    # update checkpoint
                    if self.checkpoint is not None:
                        self.checkpoint.commit()
                
                    log.info("Batch committed: {0} items created and {1} items unchanged out of {2} attempted".format(
                        len(status['success']), len(status['unchanged']), self._batch.size
                    ))
            
            except:
                log.error("Error importing {0} items to Zotero".format(self._batch.size))
                if self.checkpoint is not None:
                    self.checkpoint.rollback()
                raise
            
            finally:
                self._batch.clear()
