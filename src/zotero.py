import logging as log
from pyzotero.zotero import Zotero
from .schema import PubType

# mapping of papers2 publication types 
# to Zotero item types 
ITEM_TYPES = {
    PubType.BOOK                : 'book',
    PubType.BOOK_SECTION        : 'bookSection',
    PubType.WEBSITE             : 'webpage',
    PubType.SOFTWARE            : 'computerProgram',
    PubType.JOURNAL_ARTICLE     : 'journalArticle',
    PubType.NEWSPAPER_ARTICLE   : 'newspaperArticle',
    PubType.WEBSITE_ARTICLE     : 'webpage'
    PubType.CONFERENCE_PAPER    : 'conferencePaper'
}

# TODO: manuscript report thesis

library_id = 2082517
api_key = 'wpWBK2BxZuCjim0ghD9aUEAd'

EXTRACTORS = dict(
    
)

def extract_default(pub, key):
    pass

{u'DOI': u'',
 u'ISSN': u'',
 u'abstractNote': u'',
 u'accessDate': u'',
 u'archive': u'',
 u'archiveLocation': u'',
 u'callNumber': u'',
 u'collections': [],
 u'creators': [{u'creatorType': u'author',
                u'firstName': u'',
                u'lastName': u''}],
 u'date': u'',
 u'extra': u'',
 u'issue': u'',
 u'itemType': u'journalArticle',
 u'journalAbbreviation': u'',
 u'language': u'',
 u'libraryCatalog': u'',
 u'pages': u'',
 u'publicationTitle': u'',
 u'relations': {},
 u'rights': u'',
 u'series': u'',
 u'seriesText': u'',
 u'seriesTitle': u'',
 u'shortTitle': u'',
 u'tags': [],
 u'title': u'',
 u'url': u'',
 u'volume': u''}

class ZoteroImporter(object):
    def __init__(self, library_id, library_type, api_key, papers2):
        self.client = Zotero(args.library_id, args.library_type, args.api_key)
        self.papers2 = papers2
        self._batch = None
    
    def begin_session(self, batch_size, checkpoint):
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
                value = EXTRACTORS[key](pub, value)
            else:
                value = extract_default(pub, key)
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
                self.client.check_items(batch.keys())
                # upload metadata
                self.client.create_items(batch.keys())
                # upload attachments
        
                # update checkpoint
                self._checkpoint.commit()
        
            except:
                log.error("Error importing {0} items to Zotero".format(batch_size))
                checkpoint.rollback()
                raise
            
            finally:
                self._batch = {}
