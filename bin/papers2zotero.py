#!/usr/bin/env python
# Export publications from a Papers2 database to
# a Zotero account.

from argparse import ArgumentParser
from ConfigParser import SafeConfigParser as ConfigParser
import logging as log
import sys

from papers2.schema import Papers2, Label
from papers2.zotero import ZoteroImporter
from papers2.util import Checkpoint

def main():
    #log.basicConfig()
    #log.getLogger('sqlalchemy.engine').setLevel(log.INFO)
    
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
    parser.add_argument("-c", "--include-collections", default=None, 
        help="Comma-delimited list of collections to convert into Zotero collections")
    parser.add_argument("-f", "--papers2-folder", help="Path to Papers2 folder")
    parser.add_argument("-i", "--library-id", help="Zotero library ID")
    parser.add_argument("-k", "--keyword-types", default="user,label",
        help="Comma-delimited list of keyword types to convert into tags ('user','auto','label')")
    parser.add_argument("-l", "--label-map", default=None,
        help="Comma-delimited list of label=name pairs for converting labels (colors) to keywords")
    parser.add_argument("-L", "--label-tags-prefix", default="Label",
        help="For items with a label (i.e. color), add a tag of the form '<prefix><color>'")
    parser.add_argument("-t", "--library-type", default="user", choices=("user","group"),
        help="Zotero library type (user or group)")
    parser.add_argument("--batch-size", type=int, default=50, 
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
    
    # create checkpoint for tracking uploaded items
    checkpoint = Checkpoint(args.checkpoint_file) if args.checkpoint_file is not None else None
    
    keyword_types = args.keyword_types.split(",")
    
    add_to_collections = [] if args.no_collections else None
    if args.include_collections is not None:
        add_to_collections = args.include_collections.split(",")
    
    label_map = {}
    if args.label_map is not None:
        label_map = dict(s.split('=') for s in args.label_map.split(","))
    for label in Label.__values__:
        if label.name not in label_map:
            label_map[label.name] = "{0}{1}".format(args.label_tags_prefix, label.name)
    
    # open database
    p = Papers2(args.papers2_folder)
    
    # initialize Zotero client
    z = ZoteroImporter(args.library_id, args.library_type, args.api_key, p, 
        keyword_types, label_map, add_to_collections,
        args.batch_size, checkpoint, dryrun=args.dryrun)
    
    try:
        # TODO: add options for filtering pubs to import
        for pub in p.get_publications():
            try:
                z.add_pub(pub)

            except Exception as e:
                log.error("Error converting publication {0} to Zotero".format(pub.ROWID), exc_info=e)
    
    finally:
        p.close()
        z.close()

if __name__ == "__main__":
    main()

