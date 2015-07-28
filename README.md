# Python API for Papers2 database

This library provides a high-level interface to the Papers2 database, along with scripts to export your library to various formats.

# Installation

```python
pip install git+git://github.com/jdidion/papers2.git
```

This will install the dependencies:

* [pyzotero](https://github.com/urschrei/pyzotero)
* [sqlalchemy](http://www.sqlalchemy.org/)

# Usage

There are two ways to use papers2: the API and the export scripts. Note that this software is currently of Beta quality, and therefore bugs are not unexpected. Please open issues and/or pull requests in GitHub to report (and fix) any that you find.

## API

To interact with the database programmatically, create a new Papers2 object:

```python
from papers2.schema import Papers2
db = Papers2() # opens database at default location
```

To iterate through all the publications in the database:

```python
for pub in db.get_publications():
    print pub.title
```

The API uses SqlAlchemy auto-mappings to generate objects from database tables, so you can view the full list of available fields either by viewing `.schema` in the SQLite3 console, or by running python in an interactive session and printing the object dictionary:

```python
print dir(pub)
```

Better documentation for the API is forthcomming.

## Command Line

To simply export your library, use the executable scripts provided for each destination format. Currently, only Zotero is supported as a destination.

### Export to Zotero

You'll need three things to get started:

1. A Zotero account
2. Your Zotero user key: https://www.zotero.org/settings/keys; or, if you are importing papers into a group library, you need the group ID, which can be found in the URL of the group, e.g. https://www.zotero.org/groups/{group id}
3. A Zotero API key: https://www.zotero.org/oauth/apps

<pre>
usage: papers2zotero.py [-h] [-a API_KEY] [-C INCLUDE_COLLECTIONS]
                        [-f PAPERS2_FOLDER] [-i LIBRARY_ID] [-k KEYWORD_TYPES]
                        [-l LABEL_MAP] [-L LABEL_TAGS_PREFIX] [-r ROWIDS]
                        [-t {user,group}] [--batch-size BATCH_SIZE]
                        [--checkpoint-file CHECKPOINT_FILE]
                        [--dryrun [DRYRUN]] [--max-pubs MAX_PUBS]
                        [--attachments {all,unread,none}] [--no-collections]
                        [--log-level LEVEL] [--sql-log-level LEVEL]
                        [--http-log-level LEVEL] [-c CONFIG]

optional arguments:
  -h, --help            show this help message and exit
  -a API_KEY, --api-key API_KEY
                        Zotero API key
  -C INCLUDE_COLLECTIONS, --include-collections INCLUDE_COLLECTIONS
                        Comma-delimited list of collections to convert into
                        Zotero collections
  -f PAPERS2_FOLDER, --papers2-folder PAPERS2_FOLDER
                        Path to Papers2 folder
  -i LIBRARY_ID, --library-id LIBRARY_ID
                        Zotero library ID
  -k KEYWORD_TYPES, --keyword-types KEYWORD_TYPES
                        Comma-delimited list of keyword types to convert into
                        tags ('user','auto','label')
  -l LABEL_MAP, --label-map LABEL_MAP
                        Comma-delimited list of label=name pairs for
                        converting labels (colors) to keywords
  -L LABEL_TAGS_PREFIX, --label-tags-prefix LABEL_TAGS_PREFIX
                        For items with a label (i.e. color), add a tag of the
                        form '<prefix><color>'
  -r ROWIDS, --rowids ROWIDS
                        Comma-delimited list of database IDs of publications
                        to process.
  -t {user,group}, --library-type {user,group}
                        Zotero library type (user or group)
  --batch-size BATCH_SIZE
                        Number of articles that will be uploaded to Zotero at
                        a time.
  --checkpoint-file CHECKPOINT_FILE
                        File where list of Papers2 database IDs for
                        successfully uploaded items will be stored so that the
                        program can be stopped and resumed.
  --dryrun [DRYRUN]     Just print out the item JSON that will be sent to
                        Zotero, rather than actually sending it. If a file
                        name is specified, the JSON will be written to the
                        file rather than stdout.
  --max-pubs MAX_PUBS   Max number of publications to upload.
  --attachments {all,unread,none}
                        Which attachments to upload
  --no-collections      Do not convert Papers2 collections into Zotero
                        collections
  --log-level LEVEL     Logger level
  --sql-log-level LEVEL
                        Logger level for SQL statements
  --http-log-level LEVEL
                        Logger level for HTTP requests
  -c CONFIG, --config CONFIG
                        Configuration file
</pre>

Any of the options can be specified on the command line *or* in a config file. Values passed on the command line supersede those in the config file. Here is an example of a config file:

<pre>
[Papers2]
papers2_folder: /Users/johndoe/MyFolder/Papers2
label_map: Purple=PriorityCurrent,Red=PriorityHigh,Orange=PriorityMedium,Yellow=PriorityLow

[Zotero]
library_id: 1234567
api_key: xxxxxxxxxxxxxxxxxxxxx
</pre>

The simplest use case is when you have all your options in a config file ('config.txt'), execute the following command via the terminal:

```sh
papers2zotero.py --config config.txt
```

Some useful, yet perhaps non-obvious, additional features are:

* Collections. By default, all of the folders you created in Papers2 are replicated in Zotero as collections. If you only wish some folders to be cloned, pass the `--include-collections` option with a comma-delimited list of the folder names. If you do not wish any collections to be created, pass the `--no-collections` option.
* Keywords. There are three types of keywords in Papers2: user-defined, automatic, and labels. You are probably most familiar with user-defined keywords; when you click the "keywords" area in a paper's Info panel, you can assign keywords you've already created and/or add new keywords. Automatic keywords are extracted from the publication itself and are typically hidden from view. Labels are the 7 colors that you can assign; some people use these as a way of marking reading priority. By default, `user` and `label` keywords exported to zotero, but not `auto`; you can change this behavior by specifying a comma-delimited list of keyword types with the `--keyword-types` option. By default, label names are converted to "Label{Color}". You can change this behavior by specifying a comma-delimited list of `Color=Keyword` pairs to the `--label-map` option.
* Attachments. By default all attachments (i.e. PDF files) are uploaded to Zotero. To change this behavior, use the `--attachments` option and specify either `unread` (upload only unread attachments) or `none`.
* Checkpoint. This program exports items in batches of 50. You can change this behavior by specifying the `--batch-size` option, although 50 is the largest size (this limit is imposed by the Zotero API). Every time a batch is uploaded, the IDs of the publications that were successfully uploaded are stored to the checkpoint file. This means that you can run the program multiple times and not have to worry about the same publication being uploaded twice. By default, this file is written in the current directory to the `papers2zotero.pickle` file, but you can change this with the `--checkpoint-file` option.
* Debugging. If you'd like to test things out on a single publication or list of publications, you can do so by specifying a comma-delimited list of database IDs to the --rowids option. Currently, this requires you to open the Papers2 database with SQLite and get the ROWID field from the desired publication (i.e. `SELECT ROWID FROM Publication WHERE title='Paper Title'`). To just see the JSON that would be sent to the Zotero API without actually executing it, use the `--dryrun` option. You can pass a filename argument to `--dryrun`, in which case the JSON will be written to that file instead of stdout. You can also limit the number of publications that get exported using `--max-pubs`.
