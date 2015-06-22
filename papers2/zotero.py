from datetime import datetime
import logging as log
from pyzotero.zotero import Zotero
from .schema import PubType, IDSource

# mapping of papers2 publication types 
# to Zotero item types 
ITEM_TYPES = {
    PubType.BOOK                : 'book',
    PubType.BOOK_SECTION        : 'bookSection',
    PubType.WEBSITE             : 'webpage',
    PubType.SOFTWARE            : 'computerProgram',
    PubType.JOURNAL_ARTICLE     : 'journalArticle',
    PubType.NEWSPAPER_ARTICLE   : 'newspaperArticle',
    PubType.WEBSITE_ARTICLE     : 'webpage',
    PubType.CONFERENCE_PAPER    : 'conferencePaper'
}

class Extract(object):
    def __init__(self, fn):
        self.fn = fn
    
    def extract(self, pub, papers2):
        value = self.fn(pub)
        if isinstance(value, tuple):
            value = filter(None, value)
            return value[0] if len(value) > 0 else None
        else:
            return value

class ExtractRange(Extract):
    def extract(self, pub, papers2):
        "{0}-{1}".format(*self.fn(pub))

class ExtractTimestamp(Extract):
    def extract(self, pub, papers2):
        datetime.fromtimestamp(self.fn(pub))

class ExtractPubdate(Extract):
    def extract(self, pub, papers2):
        def _parse_datenum(s, minval, maxval):
            try:
                i = int(s)
                return None if (i < minval or i > maxval) else i
            except:
                None
            
        pub_date = self.fn(pub)
        date_str = ''
        
        year = _parse_datenum(pub_date[year[2]:year[5]+1])
        if year is not None:
            date_str = year
            
            month = _parse_datenum(pub_date[month[6]:month[7]+1])
            if month is not None:
                date_str += "-" + month
                
                day = pub_date[month[8]:month[9]+1]
                if date is not None:
                    date_str += "-" + day
        
        return date_str

class ExtractAuthors(object):
    def extract(self, pub, papers2):
        # TODO: need to handle other author types
        def _parse_author(a):
            if author.institutional > 0:
                return { 
                    u'creatorType': u'author',
                    u'name': a.surname
                }
            else:
                return { 
                    u'creatorType': u'author',
                    u'firstName': a.prename,
                    u'lastName': a.surname
                }
        
        return map(_parse_author, papers2.get_pub_authors(pub))

class ExtractPubMedID(object):
    def extract(self, pub, papers2):
        pmids = papers2.get_identifiers(pub, IDSource.PUBMED)
        return "PMID: {0}".format(pmids[0]) if len(pmids) > 0 else None

class ExtractUrl(object):
    def extract(self, pub, papers2):
        urls = papers2.get_urls(pub, IDSource.USER)
        return urls[0] if len(urls) > 0 else None
            
class AttrExtract(object):
    def __init__(self, key):
        self.key = key
    
    def extract(self, pub, papers2):
        return getattr(pub, self.key)

# TODO: item types: manuscript report thesis
# TODO: papers2 entites: label, keywords, collections, annotations/notes, review/rating
# TODO: key doesn't come back in template, but user may want to use Papers2 citekeys
# TODO: user-definable date format; for now using YYYY-MM-DD

EXTRACTORS = dict(
    DOI=                    Extract(lambda pub: pub.doi),
    abstractNote=           Extract(lambda pub: pub.summary),
    accessDate=             ExtractTimestamp(lambda pub: pub.imported_date),
    # TODO: Give the user the option of replicating Papers2 collections in Zotero
    # collections=          CollectionsExtract(),
    creators=               ExtractAuthors(),
    date=                   ExtractPubdate(lambda pub: pub.publication_date),
    extra=                  ExtractPubMedID(),
    issue=                  Extract(lambda pub: pub.number),
    journalAbbreviation=    Extract(lambda pub: pub.abbreviation),
    language=               Extract(lambda pub: pub.language),
    pages=                  ExtractRange(lambda pub: (pub.startpage, pub.endpage)),
    publicationTitle=       Extract(lambda pub: (pub.abbreviation, pub.bundle)),
    rights=                 Extract(lambda pub: pub.copyright),
    # TODO: extract tags
    # tags=                 ExtractKeywords(),
    title=                  Extract(lambda pub: pub.title),
    # TODO: what to use this for?
    # relations={}
    url=                    ExtractUrl(),
    volume=                 Extract(lambda pub: pub.volume)
)

class ZoteroImporter(object):
    def __init__(self, library_id, library_type, api_key, papers2):
        self.client = Zotero(library_id, library_type, api_key)
        self.papers2 = papers2
        self._batch = None
    
    def begin_session(self, batch_size=1, checkpoint=None):
        self._batch = {}
        self._batch_size = batch_size
        self._checkpoint = checkpoint
    
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
        template = self.client.item_template(item_type)
        
        # fill in template fields
        for key, value in template.iteritems():
            if key in EXTRACTORS:
                value = EXTRACTORS[key].extract(pub, value)
                if value is not None:
                    template[key] = value
        
        # get paths to attachments
        attachments = papers2.get_attachments(pub)
        
        # add to batch
        self._batch[template] = attachments
        
        # commit the batch if it's full
        self._commit_batch()
    
    def end_session(self):
        self._commit_batch(force=True)
        self._batch = None
        self._batch_size = None
        self._checkpont = None
        
    def _commit_batch(self, force=False):
        batch = self._batch
        batch_size = len(batch)
        if batch_size >= (1 if force else self._batch_size):
            try:
                # check that the items are valid
                #self.client.check_items(batch.keys())
                # upload metadata
                #self.client.create_items(batch.keys())
                # upload attachments
        
                # update checkpoint
                #self._checkpoint.commit()
                print batch.keys()
            except:
                log.error("Error importing {0} items to Zotero".format(batch_size))
                checkpoint.rollback()
                raise
            
            finally:
                self._batch = {}
