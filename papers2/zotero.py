from datetime import datetime
import logging as log
import sys

from pyzotero.zotero import Zotero
from .schema import PubType, IDSource, KeywordType, Label

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
                    if self.num_values is not None:
                        nvals = min(nvals, self.num_values)
                    value = self.format_tuple(value, nvals)
                    if value is not None:
                        if len(value) == 0:
                            value = None
                        elif nvals == 1:
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
        "{0}-{1}".format(*values)

class ExtractTimestamp(Extract):
    def format(self, value):
        datetime.fromtimestamp(value)

class ExtractPubdate(Extract):
    def format(self, pub_date):
        date_str = ''
        
        year = pub_date[2:6]
        if year is not None:
            date_str = year
            
            month = pub_date[6:8]
            if month is not None:
                date_str += "-" + month
                
                day = pub_date[8:10]
                if day is not None:
                    date_str += "-" + day
        
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
    def __init__(self, key):
        Extract.__init__(self, num_values=None)
    
    def get_value(self, pub, context):
        keywords = []
        if 'user' in context.keyword_types:
            keywords.extend(context.papers2.get_keywords(pub, KeywordType.USER))
        if 'auto' in context.keyword_types:
            keywords.extend(context.papers2.get_keywords(pub, KeywordType.AUTO))
        if 'label' in context.keyword_types:
            label = context.papers2.get_label_name(pub)
            if context.label_prefix is not None:
                label = "{0}{1}".format(context.label_preix, label)
            keywords.append(label)
        return keywords    

class ExtractNotes(Extract):
    def get_value(self, pub, context):
        notes = []
        
        if pub.note is not None and len(pub.note) > 0:
            note = context.client.item_template('note')
            note['note'] = pub.note
            notes.append(note)
        
        reviews = context.papers2.get_reviews(pub)
        for r in reviews:
            note = context.client.item_template('note')
            note['note'] = "{0} Rating: {1}".format(r.content, r.rating)
            notes.append(note)
        
        return notes
    
class AttrExtract(Extract):
    def __init__(self, key):
        self.key = key
    
    def get_value(self, pub, context):
        return getattr(pub, self.key)

# TODO: collections
# TODO: item types: manuscript report thesis
# TODO: handle archived papers?
# TODO: user-definable date format; for now using YYYY-MM-DD
# TODO: use relations to link book chapters to parent volume

EXTRACTORS = dict(
    DOI=                    Extract(lambda pub: pub.doi),
    ISBN=                   ExtractIdentifier((IDSource.ISBN, IDSource.ISSN)),
    abstractNote=           Extract(lambda pub: pub.summary),
    accessDate=             ExtractTimestamp(lambda pub: pub.imported_date),
    # TODO: Give the user the option of replicating Papers2 collections in Zotero
    # collections=          CollectionsExtract(),
    creators=               ExtractCreators(),
    date=                   ExtractPubdate(lambda pub: pub.publication_date),
    edition=                Extract(lambda pub: pub.version),
    extra=                  ExtractPubmedID(),
    issue=                  Extract(lambda pub: pub.number),
    journalAbbreviation=    Extract(lambda pub: pub.abbreviation),
    language=               Extract(lambda pub: pub.language),
    notes=                  ExtractNotes(),
    number=                 Extract(lambda pub: pub.document_number),
    pages=                  ExtractRange(lambda pub: (pub.startpage, pub.endpage)),
    numPages=               Extract(lambda pub: pub.startpage),
    place=                  Extract(lambda pub: pub.place),
    publicationTitle=       Extract(lambda pub: (pub.abbreviation, pub.bundle)),
    publisher=              Extract(lambda pub: pub.publisher),
    rights=                 Extract(lambda pub: pub.copyright),
    tags=                   ExtractKeywords(),
    title=                  Extract(lambda pub: pub.title),
    url=                    ExtractUrl(),
    volume=                 Extract(lambda pub: pub.volume)
)

class ZoteroImporter(object):
    def __init__(self, library_id, library_type, api_key, papers2,
            keyword_types=('user','label'), label_prefix="Label",
            add_to_collections=[], dryrun=False):
        self.client = Zotero(library_id, library_type, api_key)
        self.papers2 = papers2
        self.keyword_types = keyword_types
        self.label_prefix = label_prefix
        self.add_to_collections = add_to_collections
        self.dryrun = False
        self._batch = None
    
    def begin_session(self, batch_size=50, checkpoint=None):
        self._batch_items = []
        self._batch_attachments = []
        self._batch_collections = []
        self._batch_size = batch_size
        self._checkpoint = checkpoint
        self._collections = None
    
    # TODO: need to handle collection hierarchies
    def _load_collections(self):
        if self._collections is None:
            self._collections = {}
            if self.add_to_collections is None:
                self.add_to_collections = self.papers2.get_collections()

            if len(self.add_to_collections) > 0:
                # fetch existing zotero collections
                existing_collections = {}
                for zc in self.client.collections():
                    data = zc['data']
                    existing_collections[data['name']] = data['key']
                
                # add any papers2 collections that do not already exist
                payload = []
                for pc in self.add_to_collections:
                    if pc.name not in zc:
                        payload.append(dict(name=pc.name))
                if len(payload) > 0:
                    self.client.create_collection(payload)
                
                # re-fetch zotero collections in order to get keys
                for zc in self.client.collections():
                    data = zc['data']
                    if data['name'] in self.add_to_collections:
                        self._collections[data['name']] = data['key']
    
    def add_pub(self, pub):
        # make sure we're in a session
        assert self._batch is not None, "add_pub was called before begin_session"
        
        # ignore publications we've already imported
        if self._checkpoint.contains(pub.ROWID):
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
        
        # get paths to attachments
        attachments = self.papers2.get_attachments(pub)
        
        # add to batch
        self._batch_items.append(template)
        self._batch_attachments.append(attachments)
        
        if len(self._collections) > 0:
            pub_collections = filter(
                lambda c: c.name in self._collections,
                self.papers2.get_pub_collections(pub))
            self._batch_collections.append(pub_collections)
        
        # commit the batch if it's full
        self._commit_batch()
        sys.exit()
    
    def end_session(self):
        self._commit_batch(force=True)
        self._batch_items = None
        self._batch_attachments = None
        self._batch_collections = None
        self._batch_size = None
        self._checkpont = None
        self._collections = None
        
    def _commit_batch(self, force=False):
        batch_size = len(self._batch_items)
        if batch_size >= (1 if force else self._batch_size):
            try:
                # check that the items are valid
                self.client.check_items(self._batch_items)
                
                if self.dryrun:
                    for item, attachments, collections in zip(
                            self._batch_items, self.batch_attachments, self._batch_collections):
                        print "{0} : {1} : {2}\n".format(str(item), str(attachments), str(collections))
                
                else:
                    # upload metadata
                    status = self.client.create_items(self._batch_items)
                
                    if len(status['failed'] > 0):
                        for k,v in status['failed'].iteritems():
                            idx = int(k)
                            # remove failures from the checkpoint
                            self.checkpoint.remove(idx)
                            item = self._batch_items[idx]
                            log.error("Upload failed for item {0}; code {1}; {2}".format(
                                item['title'], v['code'], v['message']))
                
                    successes = {}
                    successes.update(stats['success'])
                    successes.update(status['unchanged'])
                
                    # upload attachments
                    for k,objKey in successes.iteritems():
                        idx = int(k)
                        attachments = self._batch_attachments[idx]
                        self.client.attachment_simple(attachments, objKey)

                        collections = self._batch_collections[idx]
                        if collections is not None and len(collections) > 0:
                            for c in collections:
                                self.client.addto_collection(c, )
                
                    # update checkpoint
                    self._checkpoint.commit()
            
            except:
                log.error("Error importing {0} items to Zotero".format(batch_size))
                checkpoint.rollback()
                raise
            
            finally:
                self._batch_items = []
                self._batch_attachments = []
                self._batch_collections = []
