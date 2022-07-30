#!/usr/bin/env python


import os, sys

SCRIPT_NAMES = ['beekeeper',
'bqviewtblcreate',
'bqviewtblscan',
'cfiltr',
'chunkr',
'collapsr',
'combinatr',
'countup',
'csvreorder',
'csvstack',
'cyclops',
'datetimecalc',
'dgenr8',
'eavesdroppr',
'expath',
'ffilter',
'fgate',
'filtr',
'get-awssecret',
'ifthen',
'ifvar',
'j2spectrum',
'j2sqlgen',
'jfiltr',
'jsonfile2csv',
'jsonkey2lower',
'jsonL2nvp',
'jsonLscan',
'jsonrec2csv',
'jtransform',
'loopr',
'makeblocks',
'manifold',
'mapname',
'mercury-version',
'mergein2j',
'mergr',
'mkcfg',
'mkspectrumdb',
'ngst',
'normalize',
'pause',
'pgexec',
'pgmeta',
'profilr',
'qanon',
'quasr',
'query2table',
'repeat',
'seesv',
'segmentr',
'sqs-consume',
'string2rec',
'svctest',
'tuple2json',
'tuplegen',
'viewtblcreate',
'warp',
'xcombine',
'xfile',
'xlseer']

DOC_REGISTRY = {

    "beekeeper": """______________________________________________

+++  Mercury script: beekeeper +++
______________________________________________

Usage:
    beekeeper [-d] --config <config_file> --target <api_target> [--r-params=<n:v>,...] [--t-params=<n:v>,...] [--macro-args=<n:v>,...]
    beekeeper [-d] -b --config <config_file> --target <api_target> --outfile <filename> [--r-params=<n:v>,...] [--t-params=<n:v>,...] [--macro-args=<n:v>,...]
    beekeeper --config <config_file> --list

Options:
    -d --debug      execute in debug mode (dump headers and template values)
    -b --bytes      return request data as bytes (as opposed to string)    


beekeeper is a command-line utility for interacting with HTTP API's.
It is driven by a YAML configuration file whose core metaphor is the <target>. 

Beekeeper targets are defined in the following structure within the YAML file:

targets:
    <target_name>:
      url: <url_endpoint>
      method: <http_method>
      headers: ## optional                                  
          <header_name>: <header_value>
          ...

      request_params:
        - name: <parameter_name>
          value: <parameter_value>
        ...

So that issuing the command

beekeeper --config <yamlfile> --target <target_name>

will issue the corresponding HTTP request to the designated URL in the configuration.
A request can have an arbitrary number of header fields and request parameters.


""",

    "bqviewtblcreate": """______________________________________________

+++  Mercury script: beekeeper +++
______________________________________________

Usage:  
    viewtblcreate --target_schema <target_schema> --tablename <table> --sqlfile=<sql_file> [--nlstrip] [--params=<n:v>...]
    viewtblcreate --target_schema <target_schema> --tablename <table> [--nlstrip] [--params=<n:v>...]


""",

    "bqviewtblscan": """______________________________________________

+++  Mercury script: beekeeper +++
______________________________________________

Usage:  
    viewtblscan --source <schemafile> --name <table_name> [--quote=<quote_char>] [--ss=<schema> --ts=<schema>] [--tprefix=<prefix,tablename>]
    viewtblscan --source <schemafile> --exportall --dir <export_dir> [--quote=<quote_char>] [--ss=<schema> --ts=<schema>] [--tprefix=<prefix,tablename>]
    viewtblscan --source <schemafile> --list
        

Options: 
    -p,--preview     : show (but do not execute) export command
    -f,--file
    -o,--output      : output the SQL for the named view-table 

""",

    "cfiltr": """______________________________________________

+++  Mercury script: cfiltr +++
______________________________________________

Usage:
    cfilter [-d <delim>] --accept-to <filename> --filter <module.function> <source_file> [--params=<name:value>...] [--limit=<limit>]
    cfilter [-d <delim>] --reject-to <filename> --filter <module.function> <source_file> [--params=<name:value>...] [--limit=<limit>]
    cfilter --config <configfile> --setup <setup_name> --source <source_file> [--params=<name:value>...] [--limit=<limit>]

Options:
    -d  --delimiter set the delimiter to the specified character (comma is default)


cfilter (CSV filter) filters a source CSV file <source_file> by processing each record through 
a filter function specified by <module.function>. 

The filter function's signature is:

filter_func(record: dict, line: str, service_registry=None, **filter_args) -> bool

where <line> is the raw CSV record and <record> is its dictionary form. The filter function is only passed a 
service registry iff cfilter is called with the --config option set; then the service registry will contain
live instances of all service objects specified in <configfile>.


if the filter function returns True, the record will be deemed as ACCEPTED. If False, the record will be deemed as REJECTED.

When the --accept-to option is set, ACCEPTED records are written to <filename> and REJECTED records are written to standard out.

When the --reject-to option is set, REJECTED records are written to <filename> and ACCEPTED records are written to standard out.

The source CSV file must contain a header; if it does not, cfilter's behavior is undefined.


""",

    "chunkr": """______________________________________________

+++  Mercury script: chunkr +++
______________________________________________

Usage:
    chunkr --filenames <listfile> --src-dir <directory> --chunks <count> --pfx <outfile_prefix> --ext <extension> [-t <outdir>] [limit=<max_files>]
    chunkr --records <datafile> --chunks <count> --pfx <outfile_prefix> --ext <extension> [-t <outdir>] [limit=<max_records>]

Options:
    -t --target-dir  send chunked datafiles to this directory


chunkr splits a list of strings and divides it into N chunks, where N is specified by 

--chunks <count>

It has two modes: filename-mode (selected by passing --filenames <listfile>) and record-mode
(selected by passing --records <datafile>).

In filename mode, it will treat the <listfile> parameter as a meta-list; that is, it will compile 
its master list of records to be chunked by reading every filename specified in <listfile>.

In record mode, it will treat <datafile> as its master list of records to be chunked.

Each chunkfile generated by chunkr will be named in the format <outfile-prefix>_N.<extension>. 
By default it will write chunkfiles to the current directory, but can write them to the directory specified
by the optional <outdir> parameter.

The optional limit=<N> parameter limits it to processing N input records.


""",

    "collapsr": """______________________________________________

+++  Mercury script: collapsr +++
______________________________________________

Usage:
    collapsr --listfile <file>
    collapsr -s

Options:
    -s --stdin    read records from standard input


collapsr takes a list of values from <file> or standard input and collapses them
down to a set of unique values. (Think SELECT DISTINCT, but for textfiles.)


""",

    "combinatr": """______________________________________________

+++  Mercury script: collapsr +++
______________________________________________

Usage:
    combinatr [-d] --delimiter <delimiter> --listfiles=<filename>... [--exclude=<tuple>...] [--regex-mask=<regex>]

Options:
    -d --debug  execute in debug mode

""",

    "countup": """______________________________________________

+++  Mercury script: countup +++
______________________________________________

Usage:
    countup --from <start_number> --to <number> [--zpad <length>]
    countup --to <number> [-z] [--zpad <length>]
    countup -s [-z] [--from <start_number>] [--zpad <length>]

Options:
    -z --zero-based  count from zero instead of one
    -s --stdin  count the number of lines sent via stdin


countup emits a list of consecutive integers (one per line) from zero or <start_number>
to <number>, inclusive. The integers can be optionally zero-padded by passing the optional

--zpad <length>

parameter.

Running countup with the -s (--stdin) parameter will cause it to emit a consecutive integer 
for each line sent to countup via standard input, starting with zero or <start_number>. This makes
it easy to (for example) generate ROWIDs, or line numbers for a text file.


""",

    "csvreorder": """______________________________________________

+++  Mercury script: countup +++
______________________________________________

Usage:
    csvreorder --output-field-list <module.list-object> --delimiter <delimiter> [-q] --datafile <filename> [--limit=<limit>]
    csvreorder --config <configfile> --setup <setup> --delimiter <delimiter> --datafile <filename> [--limit=<limit>]

Options:
    -q --quote-all    Quote all outgoing data values

""",

    "csvstack": """______________________________________________

+++  Mercury script: csvstack +++
______________________________________________

Usage:
    csvstack --listfile <filename> 
    csvstack --files <file1>...


csvstack concatenates a list of CSV files to standard output, only printing the header of the first
file in the list.

In --listfiles mode, it will read data from each CSV file listed in <filename>.
In --files mode, it will read data from each CSV file passed on the command line as a comma-separated list
(no spaces).

Note that csvstack assumes that each file will contain a single-line header. It is agnostic 
with respect to delimiters.


""",

    "cyclops": """______________________________________________

+++  Mercury script: csvstack +++
______________________________________________

Usage:
    cyclops [-d] [-p] --config <configfile> --trigger <trigger_name> [--params=<n:v>...]
    cyclops [-d] [-p] --config <configfile> --triggers <t1>...  [--params=<n:v>...]
    cyclops [-d] --config <configfile> --replay <trigger_name> <filename>  [--params=<n:v>...]
    cyclops --config <configfile> --list [-v]

Options:
    -d --debug          emit debugging information
    -p --preview        show filesystem events, but do not execute the trigger task
    -v --verbose        show event trigger details

""",

    "datetimecalc": """______________________________________________

+++  Mercury script: datetimecalc +++
______________________________________________

Usage:
    datetimecalc --days <num_days> (--before | --from) (today | now)  


datetimecalc yields a single date or datetime value <num_days> days into the future or the past.
The --before option is past-looking; the --from option is future-looking. So that


datetimecalc --days 1 --before today

gives yesterday's date, and

datetimecalc --days 1 --after today

gives tomorrow's date.


Pass: 

today 

as the last parameter and it will yield a date; pass: 

now 

and it will yield a datetime.


""",

    "dgenr8": """______________________________________________

+++  Mercury script: dgenr8 +++
______________________________________________

Usage:
    dgenr8 --plugin-module <module> --sql --schema <schema> --dim-table <tablename> --columns <columns>... [--limit=<limit>]
    dgenr8 --plugin-module <module> --sqlmulti --schema <schema> --dim-table <tablename> --columns <columns>... [--limit=<limit>]
    dgenr8 --plugin-module <module> --csv --delimiter <delimiter> --columns <columns>... [--limit=<limit>]
    dgenr8 --generator <module.function> --sql --schema <schema> --dim-table <tablename> --columns <columns>... [--limit=<limit>]
    dgenr8 --generator <module.function> --sqlmulti --schema <schema> --dim-table <tablename> --columns <columns>... [--limit=<limit>]
    dgenr8 --generator <module.function> --csv --delimiter <delimiter> --columns <columns>... [--limit=<limit>]


dgenr8 (dimension table generator) generates SQl insert statements or CSV records 
to populate OLAP star-schema dimension tables.


""",

    "eavesdroppr": """______________________________________________

+++  Mercury script: eavesdroppr +++
______________________________________________

Usage:
    eavesdroppr --config <configfile> channels
    eavesdroppr --config <configfile> -c <event_channel>
    eavesdroppr --config <configfile> -c <event_channel> -g (trigger | procedure)

Options:
    -g --generate    generate SQL LISTEN/NOTIFY code          
    -c --channel     target event channel


eavesdroppr: command-line utility for "eavesdropping" on PostgreSQL databases.
This is a code generator and dependency injector for observing Postgres changes
via LISTEN/NOTIFY. 

Event "channels" (a channel is a named context for listening to Postgres inserts/updates) are configured
in a YAML file. Run eavesdroppr in generate mode (using the -g argument) to generate the
trigger and stored procedure DDL scripts necessary for setting up listen/notify on a given channel;
then (after executing the DDL scripts), run eavesdroppr in normal mode (using the -c argument
to specify the channel). The handler in the configured channel will execute for as long as the
eavesdroppr process is running.
  

""",

    "expath": """______________________________________________

+++  Mercury script: expath +++
______________________________________________

Usage:
    expath --listfile <file>
    expath -s

Options:
    -s --stdin    read records from standard input


expath (extract path) takes a list of fully-qualified file references 
(from <file> if the --listfile option is set, or from standard input if -s is set)
and yields the paths only, stripping away the filename and the trailing slash.

The input path(s) need not be valid; that is, they CAN point to nonexistent files.


""",

    "ffilter": """______________________________________________

+++  Mercury script: expath +++
______________________________________________

Usage:
    ffilter.py --accept-to <filename> --filter <module.function> --delimiter <delimiter> <source_file> [--params=<name:value>...] [--limit=<limit>]
    ffilter.py --reject-to <filename> --filter <module.function> --delimiter <delimiter> <source_file> [--params=<name:value>...] [--limit=<limit>]
    ffilter.py --config <configfile> --setup <setup_name> --source <source_file> [--params=<name:value>...] [--limit=<limit>]

""",

    "fgate": """______________________________________________

+++  Mercury script: fgate +++
______________________________________________

Usage:
    fgate.py --config <configfile> --gate <gate> <source_file>
    fgate.py --gate-module <module> --gate-function <func> --format (csv | json) <source_file> [--delimiter=<delimiter>]
    fgate.py --gate-module <module> --service_module <svc-module> --gate-function <func> --format (csv | json) <source_file> [--delimiter=<delimiter>]


fgate (file gate): a data utility for all-or-nothing validation of a datafile.

fgate is intended to run against a source datafile (supported formats are csv and json). The user
is to supply a "gate function" which receives an open file handle and can step through the file's
contents, returning True if the file should pass (a "GO" condition) and False if it should fail
(a "NO-GO" condition).

In the case of a GO condition, fgate will emit the name of the datafile to standard out. In the case
of a NO-GO condition, fgate will return an empty string.


""",

    "filtr": """______________________________________________

+++  Mercury script: fgate +++
______________________________________________
Usage:
            filtr -x <expression> [--datafile <datafile>] [--skip <skip_count> ] [--limit=<max_records>]
            filtr -x <expression> -p 
            filtr --fmodule <filter_module> [--servicemodule <svc_module>] --ffunc <filter_function> [datafile] [--limit=<max_records>]
            filtr --config <configfile> --list (rules | functions | services)
            filtr --config <configfile> -r <filter_rule> [--limit=<max_records>]                        

   Options:
            -p   --preview      in expression mode, show the generated lambda

""",

    "get-awssecret": """______________________________________________

+++  Mercury script: fgate +++
______________________________________________

Usage:
        get-aws-secret --region <region> --secret <secret_name> --tag <tag> --section <section>
        get-aws-secret --region <region> --secret <secret_name> --tag <tag> --sections
        get-aws-secret --region <region> --secret <secret_name> --tags

""",

    "ifthen": """______________________________________________

+++  Mercury script: fgate +++
______________________________________________

Usage:
    ifthen [-n] <module.if_func> --listfile <file> --csv --delimiter <delim> --then-cmd <command> [--params=<name:value>...]
    ifthen [-n] <module.if_func> --listfile <file> --json --then-cmd <command> [--params=<name:value>...]
    ifthen [-n] <module.if_func> -s --csv --delimiter <delim> --then-cmd <command> [--params=<name:value>...] 
    ifthen [-n] <module.if_func> -s --json --then-cmd <command> [--params=<name:value>...] 

Options:
    -s --stdin  read data from standard input
    -n --not    invert mode (execute the then-cmd if the if-function is NOT true)

""",

    "ifvar": """______________________________________________

+++  Mercury script: ifvar +++
______________________________________________

Usage: 
    ifvar <env_var> --str <output_string> [--argprefix]
    ifvar <env_var> --token <vartoken> --t <output_string> [--argprefix]
    ifvar <env_var> --expr <py_expr> 

Options:
    -t --template   Use the output string as a template, subbing in the specified var if present
    -a --argprefix  Prepend a double-dash to the output string


ifvar prints a value to standard out if the environment variable <env_var> is set.

if the --str option is set, it will print <output_string> (preceded with a double-dash if
--argprefix is passed as well).

if --token is set, it will print the template <output_string>, interpolating the value of
the specified env var for <vartoken> in the template. So if the env var HOME is set,
the command

ifvar HOME --token % --t %/bin

will yield:

<your home directory>/bin

if --expr is set, it will execute the quoted python expression <py_expr>.


""",

    "j2spectrum": """______________________________________________

+++  Mercury script: ifvar +++
______________________________________________

Usage:  
    j2spectrum --config <configfile> --source <schemafile> --schema <ext_schema> --table <tablename>
    j2spectrum --config <configfile> --source <schemafile> --schema <ext_schema> --tables <comma_delim_tbl_list>
    j2spectrum --config <configfile> --source <schemafile> --schema <ext_schema> --all-tables
    j2spectrum --config <configfile> --source <schemafile> --list-tables

""",

    "j2sqlgen": """______________________________________________

+++  Mercury script: ifvar +++
______________________________________________

     Usage:  
        j2sqlgen --config <configfile> --source <schemafile> [(--schema <db_schema>)] --table <tablename>
        j2sqlgen --config <configfile> --source <schemafile> [(--schema <db_schema>)] --tables <comma_delim_tbl_list>
        j2sqlgen --config <configfile> --source <schemafile> [(--schema <db_schema>)] --all-tables
        j2sqlgen --config <configfile> --source <schemafile> --list_tables


""",

    "jfiltr": """______________________________________________

+++  Mercury script: jfiltr +++
______________________________________________

Usage:
    jfilter --accept-to <filename> --filter <module.function> <source_file> [--params=<name:value>...] [--limit=<limit>]
    jfilter --reject-to <filename> --filter <module.function> <source_file> [--params=<name:value>...] [--limit=<limit>]
    jfilter --config <configfile> --setup <setup_name> --source <source_file> [--params=<name:value>...] [--limit=<limit>]

Options:
    -c  --csv  process records as CSV


jfilter (JSON filter) filters a source JSON file <source_file> by processing each record through 
a filter function specified by <module.function>. 

The filter function's signature is:

filter_func(record: dict, line: str, service_registry=None, **filter_args) -> bool

where <line> is the raw JSON record and <record> is its dictionary form. The filter function is only passed a 
service registry iff jfilter is called with the --config option set; then the service registry will contain
live instances of all service objects specified in <configfile>.


If the filter function returns True, the record will be deemed as ACCEPTED. If False, the record will be deemed as REJECTED.

When the --accept-to option is set, ACCEPTED records are written to <filename> and REJECTED records are written to standard out.

When the --reject-to option is set, REJECTED records are written to <filename> and ACCEPTED records are written to standard out.


""",

    "jsonfile2csv": """______________________________________________

+++  Mercury script: jsonfile2csv +++
______________________________________________

Usage:
    jsonfile2csv <jsonfile> --generator <module.class> --delimiter <delimiter> [--params=<name:value>...] [--limit=<max_records>]
    jsonfile2csv --generator <module.class> --delimiter <delimiter> [--params=<name:value>...] [--limit=<max_records>]


### UNDER CONSTRUCTION


""",

    "jsonkey2lower": """______________________________________________

+++  Mercury script: jsonfile2csv +++
______________________________________________

Usage:
    jsonkey2lower --datafile <filename> [--limit <limit>]
    jsonkey2lower -s [--limit <limit>]

Options:
    -s --stdin  read data from standard input

""",

    "jsonL2nvp": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    jsonL2nvp --datafile <file> --key <key_field> --value <value_field>
    jsonL2nvp -s --key <key_field> --value <value_field>

Options:
    -s --stdin  read data from standard input


jsonL2nvp (JSONL to name-value pairs) transforms a set of JSONL records into an array of name/value pairs 
where name = source_record[<key_field>] and value = source_record[<value_field>].

So that if we start with a sample.json file containing:

{"first_name": "John", "last_name": "Smith"}
{"first_name":"Bob", "last_name":"Barker"}

and issue the command:

jsonL2nvp --datafile sample.json --key first_name --value last_name

we will receive the output:

[{"John": "Smith"}, {"Bob": "Barker"}]

jsonL2nvp reads its input records from <file> if the --datafile option is set, and
from standard input if the --stdin option is set.


""",

    "jsonLscan": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    jsonLscan -f <filename>
    jsonLscan (-s | -f <filename>) --list <field> --as <output_field> [--prefix=<prefix_string>]
    jsonLscan (-s | -f <filename>) --list <field> [-r] [--prefix=<prefix_string>]
    jsonLscan (-s | -f <filename>) --readfile <field> [--into <output_file>] [--prefix=<prefix_string>]

 Options:
    -r --raw   print the raw output value as text, not JSON
    -f --file   read the JSONL data from a file
    -s --stdin   read the JSONL data from standard input

""",

    "jsonrec2csv": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    jsonrec2csv --config <configfile> --setup <setup> --delimiter <delimiter> [--datafile <jsonfile>] [--limit=<max_records>]

""",

    "jtransform": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    jtransform --config <configfile> --map <map_name> --datafile <jsonfile> 
    jtransform --config <configfile> --map <map_name> -s
    jtransform --config <configfile> --list

Options:
    -s --stdin  read JSON records from standard input

""",

    "loopr": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    loopr [-d] [-p] -t --listfile <filename> --vartoken <token> --cmd-string <command> [--postcmd <post-command>] [--limit=<limit>]
    loopr [-d] [-p] -j --listfile <filename> --cmd-string <command> [--postcmd <post-command>] [--limit=<limit>]
    loopr [-d] [-p] -c --listfile <filename> --delimiter <delim> --cmd-string <command> [--postcmd <post-command>] [--limit=<limit>]

Options:
    -d --debug      run in debug mode (dump commands and parameters to stderr)
    -t --text       read each line from the listfile as a monolithic text value
    -c --csv        read each line from a CSV listfile as a dictionary record
    -j --json       read each line from a JSON listfile as a dictionary record
    -p --preview    show (but do not execute) the final command string

""",

    "makeblocks": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    makeblocks --type <blocktype> <blockname> [--makefile <filename>]
    makeblocks --type <blocktype> <blockname> --decode [--makefile <filename>]
    makeblocks --type <blocktype> <blockname> --decode (-s | -i) [--makefile <filename>]
    makeblocks --type <blocktype> <blockname> --decode --paramfile <file> [--makefile <filename>]
    makeblocks --type <blocktype> <blockname> --decode --params=<n:v>... [--makefile <filename>]
    makeblocks --type <blocktype> --list [--makefile <filename>]
    makeblocks --type <blocktype> --all [--makefile <filename>]
    makeblocks --scan [--makefile <filename>]
    makeblocks --check <targetblock_name> [--service-cfg <configfile>] [--makefile <filename>]
    makeblocks --check [<targetblock_name>] --list [--makefile <filename>]
    makeblocks --find --type <blocktype> <blockname> [--makefile <filename>]
    makeblocks --copytarget <targetblock_name> [--makefile <filename>]
    makeblocks --console [--makefile <filename>]

Options:
    -i --interactive    prompt user for substitution values
    -s --stdin          receive template values from stdin
    -c --console        run makeblocks in an interactive console session

""",

    "manifold": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    manifold [-d] --write --param-generator <module.generator-function> [--inputs=<name:value>...]
    manifold [-d] --read [-n] <paramfile> --warpcmd <command> --outfile-prefix <prefix> --outfile-suffix <suffix>
    manifold [-d] --read [-n] <paramfile> --warpfile <file> --outfile-prefix <prefix> --outfile-suffix <suffix>

Options:
    -n --noindex   Do not automatically generate index numbers for outfile names
    -d --debug     Run manifold in debug mode

""",

    "mapname": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    mapname <filename> --mapper-function <module.func> [--params=<name:value>...]


""",

    "mercury-version": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________
None
""",

    "mergein2j": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    mergein2j --from-csv <csvfile> --delimiter <delim> --column <colname> --into <jsonfile> [--limit=<limit>]
    mergein2j --from-csv <csvfile> --delimiter <delim> --colmap <mapfile> --into <jsonfile> [--limit=<limit>]
    mergein2j --from-csv <csvfile> --delimiter <delim> --keys=<key>... --into <jsonfile> [--limit=<limit>] 
    mergein2j --from-list <listfile> --key <key> --into <jsonfile> [--limit=<limit>]
    

""",

    "mergr": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    mergr --files=<filename>... --keys=<key>...


""",

    "mkcfg": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
  mkcfg --list
  mkcfg <target>
  mkcfg <target> --edit <configfile>

Options:
  -l --list       show the available configuration targets

""",

    "mkspectrumdb": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    mkspectrumdb --schema <schema> --db <database> --role <arn>

""",

    "ngst": """______________________________________________

+++  Mercury script: jsonL2nvp +++
______________________________________________

Usage:
    ngst --config <configfile> [-p] --target <target> [--datafile <file>] [--params=<name:value>...] [--limit=<max_records>]           
    ngst --config <configfile> --list (targets | datastores | globals)

Options:            
    -i --interactive   Start up in interactive mode
    -p --preview       Display records to be ingested, but do not ingest

""",

    "normalize": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    normalize --datafile <file>
    normalize -s

Options:
    -s --stdin  read data from standard input


normalize takes a set of lines (from a file or standard input) and performs two transforms
on each line:

- replaces whitespace with underscores; and
- changes uppercase chars to lowercase.


""",

    "pause": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    pause <pause_time_seconds>


""",

    "pgexec": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    pgexec [-p] --target <alias> --db <database> -q <query>
    pgexec [-p] --target <alias> --db <database> -f <sqlfile> [--params=<n:v>...]
    pgexec [-p] --target <alias> --db <database> -s
    pgexec -a --db <database> -q <query>
    pgexec -a --db <database> -f <sqlfile> [--params=<n:v>...]    
    pgexec --targets

Options:
    -a --anon-target    anonymous target; read credentials as JSON from stdin
    -f --file           execute contents of SQL file
    -q --query
    -s --stdin          execute SQL passed to standard input
    -p --preview        show (but do not execute) SQL command(s)

""",

    "pgmeta": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    pgmeta [--config <configfile>] --target <alias> --database <db> --schema <schema> --table <table>
    pgmeta [--config <configfile>] --target <alias> --database <db> --schema <schema> --tables <tables>...
    pgmeta [--config <configfile>] --target <alias> --database <db> --schema <schema> --tables --match=<regex>
    pgmeta [--config <configfile>] --targets
    pgmeta -a --database <db> --schema <schema> --table <table>
    pgmeta -a --database <db> --schema <schema> --tables <tables>...
    pgmeta -a --database <db> --schema <schema> --tables --match=<regex>

Options:
    -a --anon-target    anonymous target; read credentials as JSON from stdin


""",

    "profilr": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    profilr --config <configfile> --job <job_name> --format <format> --datafile <file> [--params=<name:value>...] [--limit=<limit>]
    profilr --config <configfile> --list

""",

    "qanon": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    qanon [-d] --config <configfile> --job <job> [--limit=<limit>]

Options:
    -d --debug  Run in debug mode

""",

    "quasr": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    quasr [-p] --config <configfile> --job <jobname> [--params=<name:value>...]
    quasr [-p] --config <configfile> --jobs [--file <filename>]
    quasr --config <configfile> --list [-v]

Options:
    -p --preview      preview mode
    -v --verbose      verbose job listing

""",

    "query2table": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    query2table --schema <schema> --table <table> [--query <query>]

""",

    "repeat": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    repeat --count <num_times> [--str <string>]
    repeat --linecount <file> [--str <string>]

""",

    "seesv": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________
Usage:
            seesv --xform=<transform_file> --xmap=<transform_map>  <datafile>
            seesv (-t | -f) --schema=<schema_file> --rtype=<record_type> <datafile>
            seesv -i

   Options:
            -t --test          Test the records in the target file for schema compliance
            -f --filter        Send the compliant records in the target file to stdout
            -i --interactive   Start up in interactive mode
            -l --lookup        Run seesv in lookup mode; attempt to look up missing data

""",

    "segmentr": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    segmentr --config <configfile> --job-name <jobname> --jobfile <filename> --segments=<segname>,... [--segment-params=<n,v>...] [--limit=<max_records>]
    segmentr --config <configfile> --job-name <jobname> --jobfile <filename> --segment-count=<number> [--segment-params=<n,v>...] [--limit=<max_records>]


""",

    "sqs-consume": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    sqs-consume --config <configfile> --source <source_name>
    sqs-consume --version

""",

    "string2rec": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    string2rec --str <string> --transformer <module.transform_func> [--params=<n,v>...]
    string2rec --listfile <filename> --transformer <module.function> [--params=<n,v>...] [--limit=<limit>]

""",

    "svctest": """______________________________________________

+++  Mercury script: normalize +++
______________________________________________

Usage:
    svctest --config <configfile> 
    svctest --config <configfile> --testfunc <module.function> [--params=n,v...]

""",

    "tuple2json": """______________________________________________

+++  Mercury script: tuple2json +++
______________________________________________

Usage:
    tuple2json --delimiter <delimiter> --keys=<key>... [--skip <num_lines>] [--limit=<limit>]
    tuple2json --delimiter <delimiter> --datafile <file> --keys=<key>... [--skip <num_lines>] [--limit=<limit>]



tuple2json takes a list of tuples (represented as CSV records with the specified delimiter) and turns them 
into a list of corresponding key:value JSON records whose keys are the comma-separated, ORDERED list of names
passed to the --keys parameter.

If the --datafile option is set, tuple2json reads its input records from the <file> parameter; if not, 
it reads them from standard input. 

tuple2json assumes a headlerless CSV file; it depends solely on the keys passed to it. If you are transforming a CSV
file which contains a header, you must either remove it before passing the data, or use the --skip parameter; 
otherwise the first record it generates will be a nonsense record. 

tuple2json is often used in conjunction with tuplegen to turn a set of lists into a single JSONL file.


""",

    "tuplegen": """______________________________________________

+++  Mercury script: tuplegen +++
______________________________________________

Usage:
    tuplegen --delimiter <delimiter> [--skip <num_lines>] --listfiles=<filename>... [--limit=<limit>]



tuplegen generates a (headless) set of CSV records using the specified delimiter, from the ORDERED list of files
passed to the --listfiles parameter. tuplegen will not generate a CSV header; if you wish to use it to generate
a CSV file containing a header, you must ensure that the first line in each listfile is the column name.

The source listfiles passed into tuplegen may be of different lengths; the gaps will simply be zero-length.

tuplegen is often used in conjunction with tuple2json to turn a set of lists into a single JSONL file.


""",

    "viewtblcreate": """______________________________________________

+++  Mercury script: tuplegen +++
______________________________________________

Usage:  
    viewtblcreate --target_schema <target_schema> --tablename <table> --sqlfile=<sql_file> [--nlstrip] [--params=<n:v>...]
    viewtblcreate --target_schema <target_schema> --tablename <table> [--nlstrip] [--params=<n:v>...]


""",

    "warp": """______________________________________________

+++  Mercury script: tuplegen +++
______________________________________________

Usage:
    warp [-d] (--j2 | --py) [-v] --template-file=<tfile> --show
    warp [-d] (--j2 | --py) [-v] --template=<module.name> --show
    warp [-d] (--j2 | --py) [-v] [-i] --template-file=<tfile> [--macros=<module>] [--macro-args=<name:value>...] [--params=<name:value>...]
    warp [-d] (--j2 | --py) [-v] [-i] --template=<module.name> [--macros=<module>] [--macro-args=<name:value>...] [--params=<name:value>...]
    warp [-d] (--j2 | --py) [-v] --template=<module.name> [--macros=<module>] [--macro-args=<name:value>...] -s
    warp [-d] (--j2 | --py) [-v] --template-file=<tfile> [--macros=<module>] [--macro-args=<name:value>...] -s
    warp [-d] --config <configfile> --profile <profile> [--params=<name:value...>]

Options:
    -d --debug          Execute in debug mode (dump parameters and resolved expressions to stderr)
    --py                Use python style templating
    --j2                Use Jinja2-style templating
    --show              Print (but do not process) the template
    -v --verbatim       Do not resolve embedded expressions (macros and environment vars)
    -s --stdin          Read params from stdin
    -i --interactive    Prompt the user to input template parameters 

""",

    "xcombine": """______________________________________________

+++  Mercury script: tuplegen +++
______________________________________________

Usage:
    xcombine --listfiles=<file1>... --delimiter <delim> [--working-dir <dir>]

""",

    "xfile": """______________________________________________

+++  Mercury script: tuplegen +++
______________________________________________
Usage:
            xfile --config <configfile> --delimiter <delimiter> --map <map_name> <datafile> [--limit <max_records>]
            xfile --config <configfile> --delimiter <delimiter> --map <map_name> -s [--limit <max_records>]
            xfile --config <configfile> --json --map <map_name> <datafile> [--limit <max_records>]
            xfile --config <configfile> --json --map <map_name> -s [--limit <max_records>]
            xfile --config <configfile> --list (sources | maps | globals)
            xfile -p --delimiter <delimiter> <datafile> [--limit <max_records>]
            xfile -p --json <datafile> [--limit <max_records>]

   Options:
            -s, --stream        :streaming mode (read fron stdin)
            -p, --passthrough   :passthrough mode (do not transform records)

""",

    "xlseer": """______________________________________________

+++  Mercury script: tuplegen +++
______________________________________________
Usage:
            xlseer <excel_file> sheets
            xlseer <excel_file> --sheet=<sheet> --row=<rownum>
            xlseer <excel_file> --sheet=<sheet> --rows=<x:y> [--delimiter=<delimiter_char>]
            xlseer <excel_file> --sheet=<sheet> --col=<col_id>
            xlseer <excel_file> --sheet=<sheet> --cols=<col_ids> [--delimiter=<delimiter_char>]
            xlseer <excel_file> --sheet=<sheet> --cell=<cell_id>
            xlseer <excel_file> --sheet=<sheet> --cells=<cell_range>
            xlseer -i <init_file>

""",

}
