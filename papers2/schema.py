from collections import namedtuple
import os

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import or_

from .util import enum

DEFAULTS = {
  'folder' : "~/Documents/Papers2",
}

StorageType = enum("StorageType",
    ARTICLE     = "Articles"
    BOOK        = "Books",
    MANUSCRIPT  = "Manuscripts",
    REPORT      = "reports"
)

PubAttrs = namedtuple("PubAttrs", ("name", "type", "id"))
PubType = enum('PubType',
    BOOK=               PubAttrs("Book",                StorageType.BOOK,       0),
    #BOOK_SECTION=      PubAttrs("BookSection",         StorageType.BOOK,       ??),
    THESIS=             PubAttrs("Thesis",              StorageType.BOOK        10),
    E_BOOK=             PubAttrs("eBook",               StorageType.BOOK,       20),
    WEBSITE=            PubAttrs("Website",             StorageType.LINK,       300),
    SOFTWARE=           PubAttrs("Software",            StorageType.LINK,       341),
    JOURNAL_ARTICLE=    PubAttrs("Journal Article",     StorageType.ARTICLE,    400),
    NEWSPAPER_ARTICLE=  PubAttrs("Newspaper Article",   StorageType.LINK,       402),
    WEBSITE_ARTICLE=    PubAttrs("Website Article",     StorageType.LINK,       403),
    PREPRINT=           PubAttrs("Preprint",            StorageType.ARTICLE,    415),
    CONFERENCE_PAPER=   PubAttrs("Conference Paper",    StorageType.ARTICLE,    420),
    REPORT=             PubAttrs("Report",              StorageType.REPORT,     700),
    PROTOCOL=           PubAttrs("Protocol",            StorageType.REPORT,     717)
)
pub_type_id_to_pub_type = dict((t.id,t) for t in PubType.__values__)

IDSource = enum("IDSource",
    PUBMED= "gov.nih.nlm.ncbi.pubmed",
    PMC=    "gov.nih.nlm.ncbi.pmc",
    ISBN=   "org.iso.isbn",
    ISSN=   "org.iso.issn",
    USER=   "com.mekentosj.papers2.user"
)

KeywordType = enum("KeywordType",
    AUTO = 0,
    USER = 99
)

LabelAttrs = namedtuple("LabelAttrs", ("name", "num"))
Label = enum("Label",
    NONE=       LabelAttrs("None",      0),
    RED=        LabelAttrs("Red",       1),
    ORANGE=     LabelAttrs("Orange",    2),
    YELLOW=     LabelAttrs("Yellow",    3),
    GREEN=      LabelAttrs("Green",     4),
    BLUE=       LabelAttrs("Blue",      5),
    PURPLE=     LabelAttrs("Purple",    6),
    GRAY=       LabelAttrs("Gray",      7)
)
label_num_to_label = dict((l.num, l) for l in Label.__values__)

def open_papers2(folder=None):
    if folder is None:
        folder = DEFAULTS['folder']
    return Papers2(folder)

# High-level iterface to the Papers2 database. Unless otherwise noted,
# query methods return a Query object, which can either be iterated 
# over or all rows can be fetched by calling the .all() method.
class Papers2(object):
    def __init__(self, folder):
        db = os.path.join(folder, "Library.papers2", "Database.papersdb")
        self.engine = create_engine("sqlite:///{0}".format(os.path.abspath(db)))
        self.folder = folder
        self.schema = automap_base()
        self.schema.prepare(self.engine, reflect=True)
        self._session = None
    
    def close(self):
        if self._session is not None:
            self._session.close()
    
    def get_session(self):
        if self._session is None:
            self._session = Session(self.engine)
        return self._session
    
    def get_table(self, name):
        return self.schema.classes.get(name)
    
    # Get all publications matching specified criteria.
    def get_pubs(self, types=None, include_deleted=False, include_duplicates=False):
        Publication = self.get_table("Publication")
        criteria = []
        
        if types is not None:
            types = list(t.id for t in types)
            criteria.append(Publication.subtype.in_(types))
        else:
            criteria.append(Publication.subtype.in_(pub_type_id_to_pub_type.keys()))
        
        if not include_deleted:
            criteria.append(Publication.marked_deleted == False)
        if not include_duplicates:
            criteria.append(Publication.marked_duplicate == False)
        
        q = self.get_session().query(Publication)
        if len(criteria) > 0:
            q = q.filter(*criteria)
        return q
    
    # Get the PubType for a publication
    def get_pub_type(self, pub):
        return pub_type_id_to_pub_type[pub.subtype]
    
    def get_label_name(self, pub):
        return label_name_to_laabel[pub.label].name
    
    # Get authors for a publication, in order
    def get_pub_authors(self, pub):
        Author = self.get_table("Author")
        OrderedAuthor = self.get_table("OrderedAuthor")
        return self.get_session().query(
                Author.prename.label('prename'),
                Author.surname.label('surname'),
                Author.initial.label('initial'),
                Author.fullname.label('fullname'),
                Author.affiliation.label('affiliation'),
                Author.institutional.label('institutional'),
                OrderedAuthor.type.label('type')
            ).join(OrderedAuthor, Author.ROWID == OrderedAuthor.author_id
            ).filter(OrderedAuthor.object_id == pub.ROWID
            ).order_by(OrderedAuthor.priority)
    
    # Returns SyncEvents of the given source type as a list of IDs
    def get_identifiers(self, pub, id_source):
        SyncEvent = self.get_table("SyncEvent")
        return self.get_session().query(SyncEvent).filter(
            SyncEvent.device_id == pub.uuid,
            SyncEvent.source_id == id_source)
    
    # Returns SyncEvents with remote_ids like urls ('http%'),
    # ordered by most recent
    def get_urls(self, pub):
        SyncEvent = self.get_table("SyncEvent")
        return self.get_session().query(SyncEvent
            ).filter(
                SyncEvent.device_id == pub.uuid,
                SyncEvent.remote_id.like("http%")
            ).order_by(SyncEvent.updated_at.desc())
    
    # Get all attachments, with the primary attachment first
    def get_attachments(self, pub):
        PDF = self.get_table("PDF")
        attachments = self.get_session().query(PDF
            ).filter(PDF.object_id == pub.ROWID
            ).order_by(PDF.is_primary.desc())
        # resolve relative path names
        return list((os.path.join(self.folder, a.path), a.mime_type) for a in attachments)
    
    def get_keywords(self, pub, kw_type):
        Keyword = self.get_table("Keyword")
        KeywordItem = self.get_table("KeywordItem")
        keywords = self.get_session().query(Keyword
            ).join(KeywordItem, Keyword.ROWID == KeywordItem.keyword_id
            ).filter(KeywordItem.object_id == pub.ROWID)
    
    def get_reviews(self, pub, mine_only=True):
        Review = self.get_table("Review")
        q = self.get_session().query(Review
            ).filter(Review.object_id == pub.ROWID)
        if mine_only:
            q = q.filter(Review.is_mine == 1)
        return q