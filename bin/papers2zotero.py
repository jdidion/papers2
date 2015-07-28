#!/usr/bin/env python
# Export publications from a Papers2 database to
# a Zotero account.
from argparse import ArgumentParser
import logging as log
import sys

from papers2.schema import Papers2, Label
from papers2.zotero import ZoteroImporter
from papers2.util import Checkpoint, parse_with_config

def add_arguments(parser):
    parser.add_argument("-a", "--api-key", help="Zotero API key")
    parser.add_argument("-C", "--include-collections", default=None, 
        help="Comma-delimited list of collections to convert into Zotero collections")
    parser.add_argument("-f", "--papers2-folder", default="~/Papers2", help="Path to Papers2 folder")
    parser.add_argument("-i", "--library-id", help="Zotero library ID")
    parser.add_argument("-k", "--keyword-types", default="user,label",
        help="Comma-delimited list of keyword types to convert into tags ('user','auto','label')")
    parser.add_argument("-l", "--label-map", default=None,
        help="Comma-delimited list of label=name pairs for converting labels (colors) to keywords")
    parser.add_argument("-L", "--label-tags-prefix", default="Label",
        help="For items with a label (i.e. color), add a tag of the form '<prefix><color>'")
    parser.add_argument("-r", "--rowids", default=None,
        help="Comma-delimited list of database IDs of publications to process.")
    parser.add_argument("-t", "--library-type", default="user", choices=("user","group"),
        help="Zotero library type (user or group)")
    parser.add_argument("--batch-size", type=int, default=50, 
        help="Number of articles that will be uploaded to Zotero at a time.")
    parser.add_argument("--checkpoint-file", default="papers2zotero.pickle",
        help="File where list of Papers2 database IDs for successfully uploaded items "\
             "will be stored so that the program can be stopped and resumed.")
    parser.add_argument("--dryrun", nargs="?", const="stdout", default=None,
        help="Just print out the item JSON that will be sent to Zotero, " \
             "rather than actually sending it. If a file name is specified, the JSON will be "\
             "written to the file rather than stdout.")
    parser.add_argument("--max-pubs", type=int, default=None,
        help="Max number of publications to upload.")
    parser.add_argument("--attachments", choices=("all", "unread", "none"), default="all",
        help="Which attachments to upload")
    parser.add_argument("--no-collections", action="store_true", default=False,
        help="Do not convert Papers2 collections into Zotero collections")
    parser.add_argument("--log-level", metavar="LEVEL", default="WARNING",
        choices=log._levelNames.keys(), help="Logger level")
    parser.add_argument("--sql-log-level", metavar="LEVEL", default="WARNING",
        choices=log._levelNames.keys(), help="Logger level for SQL statements")
    parser.add_argument("--http-log-level", metavar="LEVEL", default="WARNING",
        choices=log._levelNames.keys(), help="Logger level for HTTP requests")

def main():
    args = parse_with_config(add_arguments, ('Papers2', 'Zotero'))

    log.basicConfig(level=log._levelNames[args.log_level])
    log.getLogger('sqlalchemy.engine').setLevel(log._levelNames[args.sql_log_level])
    log.getLogger('requests').setLevel(log._levelNames[args.http_log_level])

    # create checkpoint for tracking uploaded items
    checkpoint = None
    if args.dryrun is None and args.checkpoint_file is not None:
        checkpoint = Checkpoint(args.checkpoint_file)
    
    keyword_types = args.keyword_types.split(",")
    
    add_to_collections = [] if args.no_collections else None
    if args.include_collections is not None:
        add_to_collections = args.include_collections.split(",")
    
    label_map = {}
    label_map[Label.NONE.name] = None
    if args.label_map is not None:
        label_map.update(dict(s.split('=') for s in args.label_map.split(",")))
    for label in Label.__values__:
        if label.name not in label_map:
            label_map[label.name] = "{0}{1}".format(args.label_tags_prefix, label.name)
    
    # open database
    p = Papers2(args.papers2_folder)
    
    # initialize Zotero client
    z = ZoteroImporter(args.library_id, args.library_type, args.api_key, p, 
        keyword_types, label_map, add_to_collections, args.attachments,
        args.batch_size, checkpoint, dryrun=args.dryrun)
    
    # Limit the number of publications to process
    # TODO: add additional options for filtering pubs to import
    query_args = {}
    max_pubs = args.max_pubs
    row_ids = None
    if args.rowids is not None:
        query_args['row_ids'] = map(int, args.rowids.split(","))
        num_ids = len(query_args['row_ids'])
        if max_pubs is None or max_pubs > num_ids:
            max_pubs = num_ids
    
    # Prepare query
    q = p.get_publications(**query_args)
    
    if max_pubs is None:
        max_pubs = q.count()
    
    num_added = 0
    
    for pub in q:
        try:
            if z.add_pub(pub):
                log.debug(u"Added to batch: {0}".format(pub.title))
                num_added += 1
            
            if max_pubs is not None and num_added >= max_pubs:
                break

        except Exception as e:
            log.error("Error converting publication {0} to Zotero".format(pub.ROWID), exc_info=e)

    p.close()
    z.close()

    log.info("Exported {0} papers to Zotero".format(num_added))

if __name__ == "__main__":
    main()

