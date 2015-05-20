from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

DEFAULTS = {
  'dbpath' : "~/Documents/Papers2/Library.papers2/Database.papersdb",
}

def open_papers2(db=None):
    if db is None:
        db = DEFAULTS['dbpath']
    engine = create_engine(db)
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
    
    