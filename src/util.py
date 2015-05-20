import os
import pickle

class Checkpoint(object):
    def __init__(self, filename):
        self.filename = filename
        if os.path.exists(filename):
            self.ids = pickle.load(filename)
        else:
            self.ids = set()
        self._uncommitted = []
    
    def add(self, db_id):
        self._uncommitted.append(db_id)
    
    def commit(self):
        self.ids.update(self._uncommitted)
        pickle.dump(self.ids, self.filename)
        self._uncommitted = []
    
    def rollback(self):
        self._uncommitted = []
    
    def contains(self, db_id):
        return db_id in self.ids

def enum(name, **enums):
    _enums = enums.copy()
    _enums["__names__"] = list(n for n in enums.keys())
    _enums["__values__"] = list(v for v in enums.values())
    _enums["__reverse_dict__"] = dict((value, key) for key,value in enums.iteritems())
    return type(name, (), _enums)