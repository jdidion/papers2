# Python API for Papers2 database

This library provides a high-level interface to the Papers2 database, along with scripts to export your library to various formats.

# Installation

```python
pip install git+git://github.com/jdidion/papers2.git@v1
```

This will install the dependencies:

* [pyzotero](https://github.com/urschrei/pyzotero)

# Usage

There are two ways to use papers2: the API and the export scripts. 

To interact with the database programmatically, create a new Papers2 object. See the documentation for a list of all operations you can perform.

```python
from papers2.schema import open_papers2
db = open_papers2() # opens database at default location
```

To simply export your library, use the executable scripts provided for each destination format. Currently, only Zotero is supported, and only a full export of your library. In future, there will be support other export formats and for export filters.

```sh
papers2zotero.py -d /path/to/Database.papersdb -i <zotero library ID> -k <zotero API key> -t <zotero library type>
```