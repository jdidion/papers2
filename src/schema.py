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

def open_papers2(db=None):
    if db is None:
        db = DEFAULTS['dbpath']
    engine = create_engine("sqlite:///{0}".format(os.path.abspath(db)))
    schema = automap_base()
    schema.prepare(engine, reflect=True)
    return Papers2(engine, schema)

class Papers2(object):
    def __init__(self, engine, schema):
        self.engine = engine
        self.schema = schema
    
    def get_session(self):
        return Session(self.engine)
    
    def get_table(self, name):
        return self.schema.classes.get(name)
    
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
    
    def get_pub_type(self, pub):
        return pub_type_id_to_pub_type[pub.subtype]
        