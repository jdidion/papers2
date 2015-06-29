#!/usr/bin/env python
from argparse import ArgumentParser
import logging as log
from papers2.schema import open_papers2
from papers2.zotero import ZoteroImporter
from papers2.util import Checkpoint
from ConfigParser import SafeConfigParser as ConfigParser
import sys

# TODO: add options for filtering pubs to import
    
def main():
    log.basicConfig()
    log.getLogger('sqlalchemy.engine').setLevel(log.INFO)
    
    pre_parser = ArgumentParser()
    pre_parser.add_argument("-c", "--config", default=None, help="Configuration file")
    args, remaining = pre_parser.parse_known_args()
    
    parser = ArgumentParser()
    if args.config is not None:
        config = ConfigParser()
        config.read(args.config)
        defaults = {}
        for section in ('Papers2', 'Zotero'):
            defaults.update(config.items(section))
        parser.set_defaults(**defaults)

    parser.add_argument("-a", "--api-key", help="Zotero API key")
    parser.add_argument("-c", "--create-collections", default=None, 
        help="Comma-delimited list of collections to convert into Zotero collections")
    parser.add_argument("-f", "--papers2-folder", help="Path to Papers2 folder")
    parser.add_argument("-i", "--library-id", help="Zotero library ID")
    parser.add_argument("-k", "--keyword-types", default="user,label",
        help="Comma-delimited list of keyword types to convert into tags ('user','auto','label')")
    parser.add_argument("-l", "--label-tags-prefix", default="Label",
        help="For items with a label (i.e. color), add a tag of the form '<prefix><color>'")
    parser.add_argument("-t", "--library-type", default="user", choices=("user","group"),
        help="Zotero library type (user or group)")
    parser.add_argument("--batch-size", type=int, default=10, 
        help="Number of articles that will be uploaded to Zotero at a time.")
    parser.add_argument("--checkpoint-file", default="papers2zotero.pickle",
        help="File where list of Papers2 database IDs for successfully uploaded items "\
             "will be stored so that the program can be stopped and resumed.")
    parser.add_argument("--dryrun", action="store_true", default=False,
        help="Just print out the item JSON that will be sent to Zotero, " \
             "rather than actually sending it.")
    parser.add_argument("--no-collections", action="store_true", default=False,
        help="Do not convert Papers2 collections into Zotero collections")
    args = parser.parse_args(args=remaining)

    p = open_papers2(args.papers2_folder)
    
    checkpoint = None
    if args.checkpoint_file is not None:
        checkpoint = Checkpoint(args.checkpoint_file)
    
    keyword_types = args.keyword_types.split(",")
    add_to_collections = [] if args.no_collections else None
    if args.create_collections is not None:
        add_to_collections = args.create_collections.split(",")
        
    z = ZoteroImporter(args.library_id, args.library_type, args.api_key, p, 
        keyword_types, label_prefix, add_to_collections, dryrun=args.dryrun)
    
    try:
        z.begin_session(args.batch_size, checkpoint, add_to_collections)
        
        for pub in p.get_pubs():
            try:
                z.add_pub(pub)

            except Exception as e:
                log.error("Error converting publication {0} to Zotero".format(pub.ROWID), exc_info=e)
                sys.exit()
        
    finally:
        p.close()
        z.end_session()

if __name__ == "__main__":
    main()

