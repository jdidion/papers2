from argparse import ArgumentParser
from papers2.schema import open_papers2
from pyzotero import zotero

def export(library_id, api_key, db_path=None, library_type="user", batch_size=10, checkpoint=None):
    p = open_papers2(db_path)
    z = zotero.Zotero(library_id, library_type, api_key)
    



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
    
    export(args.library_id, args.api_key, args.database, args.library_type,
        args.batch_size, checkpoint)

if __name__ == "__main__":
    main()

