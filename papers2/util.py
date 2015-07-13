import json
import os
import pickle
import sys

class Batch(object):
    def __init__(self, max_size):
        self.items = []
        self.attachments = []
        self.max_size = max_size
    
    @property
    def size(self):
        return len(self.items)
    
    @property
    def is_full(self):
        return self.size >= self.max_size
    
    @property
    def is_empty(self):
        return len(self.items) == 0
    
    def add(self, item, attachments):
        self.items.append(item)
        self.attachments.append(attachments)
    
    def iter(self):
        for item in zip(self.items, self.attachments):
            yield item
    
    def clear(self):
        self.items = []
        self.attachments = []

# Simple checkpointing facility that maintains a
# set of items IDs and pickles them on commit.
class Checkpoint(object):
    def __init__(self, filename):
        self.filename = filename
        if os.path.exists(filename):
            with open(filename, "rb") as i:
                self.ids = pickle.load(i)
        else:
            self.ids = set()
        self._uncommitted = []
    
    def add(self, db_id):
        self._uncommitted.append(db_id)
    
    def remove(self, db_id):
        del self._uncommitted[db_id]
    
    def commit(self):
        self.ids.update(self._uncommitted)
        with open(self.filename, 'wb') as o:
            pickle.dump(self.ids, o)
        self._uncommitted = []
    
    def rollback(self):
        self._uncommitted = []
    
    def contains(self, db_id):
        return db_id in self.ids

# Create an enumerated type
def enum(name, **enums):
    _enums = enums.copy()
    _enums["__names__"] = list(n for n in enums.keys())
    _enums["__values__"] = list(v for v in enums.values())
    _enums["__reverse_dict__"] = dict((value, key) for key,value in enums.iteritems())
    return type(name, (), _enums)
    
class JSONWriter(object):
    def __init__(self, file):
        self._fh = sys.stdout if file == "stdout" else open(file, "w")
    
    def close(self):
        if self._fh != sys.stdout:
            self._fh.close()
    
    def write(self, item, attachments):
        self._fh.write(json.dumps(item, indent=4, separators=(',', ': ')))
        self._fh.write("\n")