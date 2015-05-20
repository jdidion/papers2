#!/usr/bin/env python
from argparse import ArgumentParser
import logging as log
from papers2.schema import open_papers2
from papers2.zotero import ZoteroImporter

# TODO: add options for filtering pubs to import
    
def main():
    parser = ArgumentParser()
    parser.add_argument("-d", "--database", default=None, help="Path to Papers2 database")
    parser.add_argument("-i", "--library-id", required=True, help="Zotero library ID")
    parser.add_argument("-k", "--api-key", required=True, help="Zotero API key")
    parser.add_argument("-t", "--library-type", default="user", choices=("user","group"),
        help="Zotero library type (user or group)")
    parser.add_argument("-b", "--batch-size", type=int, default=10, 
        help="Number of articles that will be uploaded to Zotero at a time.")
    parser.add_argument("-c", "--checkpoint-file", defaut="papers2zotero.pickle",
        help="File where list of Papers2 database IDs for successfully uploaded items "\
             "will be stored so that the program can be stopped and resumed.")
    args = parser.parse_args()
    
    checkpoint = None
    if args.checkpoint_file is not None:
        checkpoint = Checkpoint(args.checkpoint_file)

    p = open_papers2(args.database)
    z = ZoteroImporter(args.library_id, args.library_type, args.api_key, p)
    
    try:
        z.begin_session(args.batch_size, checkpoint)
        
        for pub in p.get_pubs():
            try:
                z.add_pub(pub)

            except Exception as e:
                log.error("Error converting publication {0} to Zotero".format(pub.ROWID), e)
        
    finally:
        p.close()
        z.end_session()

if __name__ == "__main__":
    main()

