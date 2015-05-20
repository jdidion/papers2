import os
import pickle

class Checkpoint(object):
    def __init__(self, filename):
        self.filename = filename
        if os.path.exists(filename):
            self.ids = pickle.load(filename)
        else:
            self.ids = set()
    
    def add(self, db_id):
        self.ids.update(ids)
        pickle.dump(self.ids, self.filename)
    
    def check(self, db_id):
        return db_id in self.ids
