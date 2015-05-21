from collections import namedtuple
import os

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from .util import enum

DEFAULTS = {
  'dbpath' : "~/Documents/Papers2/Library.papers2/Database.papersdb",
}

PubAttrs = namedtuple("PubAttrs", ("name", "id"))
PubType = enum('PubType',
    BOOK=               PubAttrs("Book",                0),
    BOOK_SECTION=       PubAttrs("Book Section",        -1000),
    WEBSITE=            PubAttrs("Website",             300),
    SOFTWARE=           PubAttrs("Software",            341),
    JOURNAL_ARTICLE=    PubAttrs("Journal Article",     400),
    NEWSPAPER_ARTICLE=  PubAttrs("Newspaper Article",   402),
    WEBSITE_ARTICLE=    PubAttrs("Website Article",     403),
    CONFERENCE_PAPER=   PubAttrs("Conference Paper",    420)
)
pub_type_id_to_pub_type = dict((t.id,t) for t in PubType.__values__)

IDSource = enum("IDSource",
    PUBMED= "gov.nih.nlm.ncbi.pubmed",
    USER=   "com.mekentosj.papers2.user"
)

def open_papers2(db=None):
    if db is None:
        db = DEFAULTS['dbpath']
    engine = create_engine("sqlite:///{0}".format(os.path.abspath(db)))
    schema = automap_base()
    schema.prepare(engine, reflect=True)
    return Papers2(engine, schema)

# High-level iterface to the Papers2 database. Unless otherwise noted,
# query methods return a Query object, which can either be iterated 
# over or all rows can be fetched by calling the .all() method.
class Papers2(object):
    def __init__(self, engine, schema):
        self.engine = engine
        self.schema = schema
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
        criteria = []
        if types is not None:
            types = list(t.id for t in types)
            criteria.append(Publication.subtype in types)
        if not include_deleted:
            criteria.append(Publication.marked_deleted is False)
        if not include_duplicated:
            criteria.append(Publication.marked_duplicate is False)
        
        Publication = self.get_table("Publication")
        self.get_session().query(Publication).filter(*criteria)
    
    # Get the PubType for a publication
    def get_pub_type(self, pub):
        return pub_type_id_to_pub_type[pub.subtype]
    
    # Get authors for a publication, in order
    def get_pub_authors(self, pub):
        Author = self.get_table("Author")
        OrderedAuthor = self.get_table("OrderedAuthor")
        authors = self.get_session().query(OrderedAuthor
            ).join(Author, OrderedAuthor.author_id == Author.ROWID
            ).filter(OrderedAuthor.object_id == pub.ROWID
            ).order_by(OrderedAuthor.priority)
    
    # Returns identifiers of the given source type as a list of IDs
    def get_identifier(self, pub, id_source):
        SyncEvent = self.get_table("SyncEvent")
        self.get_session().query(SyncEvent).add_columns(SyncEvent.remote_id).filter(
            SyncEvent.device_id == pub.uuid,
            SyncEvent.source_id == id_source,
            SyncEvent.subtype == SyncEventSubtype.IDENTIFIER
        ).all()