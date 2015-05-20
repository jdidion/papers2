#!/usr/bin/env python

"""
Drives functionality that interacts with the Papers2 database.

Requires Python >= 2.5 and Papers >= 2.0.8

Copyright 2011 Steve Lianoglou, all rights reserved

License: GPL
"""

import re, os, time, sys, glob, itertools, sqlite3
from optparse import OptionParser
from ConfigParser import ConfigParser, NoOptionError

## You can overide these values in ~/.papersc
DEFAULTS = {
  'dbpath' : "~/Documents/Papers2/Library.papers2/Database.papersdb",
}

def filter_files(filelist):
    """Returns a list of files that can be 'found'"""
    found = []
    if len(filelist) == 0:
        return found
    for infile in filelist:
        if os.path.isfile(infile):
            found.append(infile)
    return found

def dict_factory(cursor, row):
    """Used to extract results from a sqlite3 row by name"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

###############################################################################
## Interface to Papers
class Papers(object):
    """Interface to Papers2.app"""
    
    _xlate_month = {
        '01' : 'Jan', '02' : 'Feb', '03' : 'Mar', '04' : 'Apr',
        '05' : 'May', '06' : 'Jun', '07' : 'Jul', '08' : 'Aug',
        '09' : 'Sep', '10' : 'Oct', '11' : 'Nov', '12' : 'Dec'
    }
    
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.dbconn = sqlite3.connect(dbpath)
        
        ## Checks to see if this is a valid db connection
        c = self.dbconn.cursor()
        try:
            c.execute("SELECT * FROM metadata LIMIT 1;")
        except sqlite3.OperationalError:
            raise ValueError("Invalid Papers database")
        self.dbconn.row_factory = dict_factory
    
    def parse_publication_date(self, pub_date, translate_month=True, month=(6,7),
                               year=(2,5)):
        """99200406011200000000222000 == Jun 2004
        returns (month, year) as strings
        """
        try:
            cmonth = pub_date[month[0]:month[1]+1]
            if translate_month:
                cmonth = Papers._xlate_month[cmonth]
        except:
            cmonth = ''
        try:
            cyear = pub_date[year[0]:year[1]+1]
        except:
            cyear = ''
        return {'month' : cmonth, 'year' : cyear}
    
    def query_papers_by_citekey(self, citekeys, n=100):
        """Returns summary information for each paper matched to citekey(s).
        
        The returned object is a `dict` keyed on the citekey for each paper,
        the values are dicts with the following minimal paper info:
        
          - title   :
          - authors : Firstname Lastname, First Last, and First Last
          - journal : Journal name (as listed in Publications db), this can
                      be done better by JOINing against NameVariant, but we
                      are not doing that for now
          - citekey : The citekey
          
          And optionally, if these are found in the Publication record:
          
          - volume  : Journal volume
          - number  : Journal number
          - pages   : start--end pages
          - month   : Month of publication date (as 3 letter name)
          - year    : 4 digit (character) year of publication
        """
        query = """SELECT publication_date, full_author_string,
                   attributed_title, bundle_string, volume, number,
                   startpage, endpage, citekey
                   FROM Publication WHERE citekey IN (%s)"""
        results = {}
        c = self.dbconn.cursor()
        while len(citekeys) > 0:
            take = min(len(citekeys), n)
            cites = ['"%s"' % x for x in citekeys[0:take]]
            cites = ','.join(cites)
            citekeys = citekeys[take:]
            c.execute(query % cites)
            for row in c:
                date = self.parse_publication_date(row['publication_date'])
                citekey = row['citekey']
                entry = {
                  'title' : row['attributed_title'],
                  'author' : row['full_author_string'],
                  'journal' : row['bundle_string'],
                  'citekey' : citekey
                }            
                if date['month'] is not None:
                    entry['month'] = date['month']
                if date['year'] is not None:
                    entry['year'] = date['year']
                if row['number'] is not None:
                    entry['number'] = row['number']
                if row['volume'] is not None:
                    entry['volume'] = row['volume']
                if row['startpage'] is not None and row['endpage'] is not None:
                    entry['pages'] = "%s--%s" % (row['startpage'], row['endpage'])
                results[citekey] = entry
        return results

# END : Class Papers

class PapersOptionParser(OptionParser, object):
    """Documentation for PapersOptionParser"""
    
    def __init__(self, usage=None):
        super(PapersOptionParser, self).__init__(usage=usage)
        self.add_option('-o', '--out', dest="out", default=None,
                        help="The file to save the output to, defaults " \
                             "to STDOUT")
        self.add_option('-d', '--dbpath', dest="dbpath", default=None,
                        help="The path to the Papers2 sqlite database, "  \
                             "defaults to [%s]. If this is set, it will " \
                             "override the value set in your ~/.papersrc" \
                             "file." % DEFAULTS['dbpath'])
        self.add_option('-v', '--verbose', action='store_true', default=False,
                        help='Make some noise')
        self.add_option('-c', '--config', default="~/.papersrc",
                        help="The path to the papers utility config file")
        self.add_option('-f', '--force', dest='force', default=False,
                        action='store_true',
                        help="Set to force overwrite of existing output file")
        self.out = None
        self.report = None
        self.to_stdout = None
                        
    
    def parse_args(self, args=None, values=None):
        """Parses the arguments.
        
        The precedence of arguments that get stuffed into `options` are
        (from highest to lowest):
        
          - values passed in through command line args/flags
          - values set in ~/.papersrc
          - DEFAULTS
        """
        (options, args) = super(PapersOptionParser, self).parse_args(args, values)
        
        if options.out is None:
            self.to_stdout = True
            self.out = sys.stdout
            self.report = sys.stderr
        else:
            if os.path.isfile(options.out) and not options.force:
                self.error("Outfile already exists. Use --force to override")
            self.to_stdout = False
            self.out = open(options.out, 'w')
            self.report = sys.stdout
        
        ## override options with values in ~/.papersrc
        config_file = os.path.expanduser(options.config)
        if os.path.isfile(config_file):
            cparser = ConfigParser()
            cparser.read(config_file)
            if options.dbpath is None:
                try:
                    options.dbpath = cparser.get('appinfo', 'dbpath')
                except NoOptionError:
                    pass
        
        if options.dbpath is None:
            options.dbpath = DEFAULTS['dbpath']
        
        return (options, args)
    
    def cleanup(self):
        if not self.to_stdout:
            self.out.close()

# END : Class PapersOptionParser


###############################################################################
## BibTex Generator
## Generates rudimentary bibtex file by parsing \cite*{}
## references in a document(s), and crossreferences the citekeys
## with the ones in your Papers2 databse.
## 
## 
## Minimal BibTex entry looks like so:
## 
## @article{Proudfoot:2004gs, citekey
## author = {Proudfoot, Nick}, author_string
## title = {{New perspectives on connecting ...}}, attributed_title
## journal = {Current opinion in cell biology}, bundle_string
## year = {2004},  publication_date (99200406011200000000222000)
## month = {jun},
## volume = {16}, volume
## number = {3}, number
## pages = {272--278} startpage -- endpage
## }
## 
## To get the journal name, use the `bundle` column in Pulblication and join it
## to NameVariant.object_id
## 
## select
##   p.publication_date, p.author_string, p.attributed_title,
##   p.bundle, p.bundle_string, p.volume, p.number, p.startpage,
##   p.endpage, n.name
## from
##   Publication as p
## inner join NameVariant as n on p.bundle=n.object_id
## where
##   p.citekey="Sandberg:2008ks";
## 
## Forget the complex query, just use bundle_string for journal name
class BibtexOptionParser(PapersOptionParser):
    """OptionParser for the bibtex command"""
    
    usage = """usage: %prog bibtex [OPTIONS] FILE1 [FILES ...]
    
    Parses the file(s) identified by the unix blob-like matching patterns
    provided in the positional arguments for cite*{...} commands in
    them and generates a minimal bibtex file for them by looking up
    the citekeys in your Papers2 database.
    
    If a -o/--out BIBFILE.tex option is not provided, the bibtex file will
    be streamed to STDOUT."""
    
    def __init__(self):
        super(BibtexOptionParser, self).__init__(usage=BibtexOptionParser.usage)
        self.infiles = []
    
    def parse_args(self, args=sys.argv[2:], values=None):
        (options, args) = super(BibtexOptionParser, self).parse_args(args, values)
        
        ## OptionParser already matches and expands unix globs for us!
        ## match input files and flatten + uniqify potentiall nested list
        ## infiles = [glob.glob(fn) for fn in args]
        ## infiles = set(itertools.chain(*infiles))
        self.infiles = filter_files(args)
        if len(self.infiles) == 0:
            self.error("Valid input file list required (no files found)")
        return (options, args)
    
            
# END : Class BibtexOptionParser

class BibtexGenerator(object):
    """Generats bibtex file from input"""
    
    citekey_regex = re.compile(r"""\\cite(?:t|p)?\{(.*?)\}""", re.MULTILINE)
    
    def __init__(self, app, infiles, author_style="default"):
        self.app = app
        self.infiles = filter_files(infiles)
        self.author_style = author_style
        self.citekeys = {}
    
    def extract_citekeys_from_line(self, line, store=True, regex=None):
        if regex is None:
            regex = BibtexGenerator.citekey_regex
        citations = regex.findall(line)
        citekeys = []
        if len(citations) > 0:
            for citation in citations:
                for citekey in citation.split(','):
                    citekey = citekey.strip()
                    citekeys.append(citekey)
                    if store:
                        try:
                            self.citekeys[citekey] += 1
                        except KeyError:
                            self.citekeys[citekey] = 1
        return citekeys
                    
    def extract_citekeys_from_file(self, infile, store=True, regex=None):
        if regex is None:
            regex = BibtexGenerator.citekey_regex
        allkeys = []
        fh = open(infile, 'r')
        for line in fh:
            allkeys.append(self.extract_citekeys_from_line(line, store=store))
        fh.close()
        return allkeys
    
    def extract_citekeys(self, infiles=None):
        """Extracts the citekeys for `infiles`"""
        if infiles is None:
            infiles = self.infiles
        else:
            infiles = filter_files(infiles)
        if len(self.infiles) == 0:
            raise ValueError("No input files found")
        for infile in infiles:
            self.extract_citekeys_from_file(infile, store=True)
    
    def convert_author_style(self, author_string, style=None):
        if style is None:
            style = self.author_style
        if style == "default":
            authors = re.sub(r"\Wand\W", " ", author_string).split(',')
            mangled = list()
            for author in authors:
                pieces = author.strip().split()
                lastname = pieces[-1]
                rest = ' '.join(pieces[:-1])
                mangled.append("%s, %s" % (lastname, rest))
            author_string = ' and '.join(mangled)
        return author_string
    
    def as_bibtex(self, info):
        result = []
        header = '@article{%s,\n' % info['citekey'].encode('utf-8')
        info['author'] = self.convert_author_style(info['author'])
        for key in info:
            if key == 'citekey':
                continue
            if key == 'title':
                add = 'title = {{%s}}' % info['title'].encode('utf-8')
            else:
                add = '%s = {%s}' % (key, info[key].encode('utf-8'))
            result.append(add)
        meta = ",\n".join(result)
        result = header + meta + "\n}\n"
        return result
    
    def generate_bibtex(self, fhandle):
        """Dumps the generated bibtex file into fhandle"""
        citations = self.app.query_papers_by_citekey(self.citekeys.keys())
        for citation in citations:
            fhandle.write(self.as_bibtex(citations[citation]))
            fhandle.write("\n")

# END : Class BibtexGenerator

## Drivers -- all these functions must accept a Papers (app) object as
## their single parameter

def do_bibtex(app):
    """Run the bibtex command"""
    parser = BibtexOptionParser()
    (options, args) = parser.parse_args()
    
    report = parser.report
    outfile = parser.out
    
    # try:
    #     app = Papers(options.dbpath)
    # except sqlite3.OperationalError:
    #     parser.error("Problem connecting to database, is the following " \
    #                  "path to your Database.papersdb database correct?\n" \
    #                  "\t%s\n" % options.dbpath)
    
    if options.verbose:
        report.write("Parsing files: " + ','.join(parser.infiles) + "\n")
    
    bibtex = BibtexGenerator(app, parser.infiles)
    bibtex.extract_citekeys()
    bibtex.generate_bibtex(outfile)
    
    if options.verbose:
        report.write("=== Citekeys Used ===\n")
        for citation, count in bibtex.citekeys.iteritems():
            report.write("%s : %d\n" % (citation, count))
    
    parser.cleanup()
    

if __name__ == '__main__':
    usage = """usage: %prog COMMAND [OPTIONS] ARGS
    
    This tool is a wrapper for (eventually) several COMMANDs that query your
    Papers2 database. Try `%prog COMMAND --help` for help for the specific
    COMMANDs that are listed below.
    
    Commands
    --------
        - bibtex : Generates a bibtex file by parsing the references in the
                   files provided in ARGS
    """
    
    commands = {'bibtex' : do_bibtex}
    
    usage = usage.replace("%prog", os.path.basename(sys.argv[0]))
    parser = PapersOptionParser(usage=usage)
    (options, args) = parser.parse_args()
    
    if len(args) == 0:
        user_cmd = ''
    else:
        user_cmd = args[0]
    
    if user_cmd not in commands:
        if len(user_cmd) > 0:
            user_cmd = "'%s'" % user_cmd
        parser.error("Unknown command %s\n" % user_cmd)
    
    try:
        app = Papers(options.dbpath)
    except ValueError:
        parser.error("Problem connecting to database, is the following " \
                     "path to your Database.papersdb database correct?\n" \
                     "\t%s\n" % options.dbpath)

    commands[user_cmd](app)