#!/usr/bin/env python


import os, sys

SCRIPT_NAMES = ['beekeeper',
'bqviewtblcreate',
'bqviewtblscan',
'cfiltr',
'chunkr',
'collapsr',
'countup',
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
'mergein2j',
'mergr',
'mkcfg',
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

+++  Mercury script: cyclops +++
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


cyclops is a filesystem watcher. Based on the configuration file, it will run in an infinite loop
and wait for a specified change to happen to a watched file or directory, then pass the
change notification to a user-defined handler function.


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

When the --plugin-module option is set, dgenr8 will load the python module designated by <module>
and look for a user-defined function called "line_array_generator". This function takes no arguments and should
be an actual Python generator yielding, per-call, an array of N ORDERED values corresponding to the N columns in the 
dimension table you wish to populate.

The value-arrays yielded by the generator will be used to create either:

- a SQL insert statement per array (if the --sql option is set),
- one bulk SQL insert statement (if the --sql-multi option is set), or
- a CSV file containing all the generated values.

Use the --dim-table option to set the name of the table to be populated in the generated SQL. 

No matter which option is used, the comma-separated list of dim-table column names will be used so that the 
emitted script represents a data structure compatible with the table's structure. The user is responsible to 
ensure that the value-arrays yielded by the line_array_generator function properly match the designated
column-order.



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
This is a code generator and dependency injector for observing Postgres DB changes
(inserts or updates) via the PostgreSQL LISTEN/NOTIFY service. 

Event "channels" (a channel is a named context for listening to Postgres inserts/updates) are set up
in a YAML configuration file, using the following structure:


channels:
  <channel_name>:
          handler_function: <function_name>
          db_table_name: <table_under_observation>
          db_operation: (INSERT or UPDATE)
          pk_field_name: <name_of_primary_key_field>
          pk_field_type: <column type of primary key field>
          db_schema: <schema>
          db_proc_name: <procedure_name> ## optional; autogenerated if blank
          db_trigger_name: <trigger_name> ## optional; autogenerated if blank

          payload_fields:
            - <column_1>
            - <column_2>
            ...            

To use eavesdroppr, once you have created a YAML configuration:

1. Run eavesdroppr in generate mode (using the -g option) to generate the
   trigger and stored procedure DDL scripts necessary for setting up listen/notify on a given channel.

2. Execute the DDL scripts, using a database client of your choice. 

3. Run eavesdroppr in normal mode (using the -c option to specify the channel). 


eavesdroppr with start up and enter an infinite loop. The handler function in the configured channel 
will execute each time the database commits an INSERT or UPDATE against the designated table (as configured), 
and will continue to do so for as long as the eavesdroppr process is running.
  

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

+++  Mercury script: filtr +++
______________________________________________
Usage:
            filtr -x <expression> [--datafile <datafile>] [--skip <skip_count> ] [--limit=<max_records>]
            filtr -x <expression> -p 
            filtr --fmodule <filter_module> [--servicemodule <svc_module>] --ffunc <filter_function> [datafile] [--limit=<max_records>]
            filtr --config <configfile> --list (rules | functions | services)
            filtr --config <configfile> -r <filter_rule> [--limit=<max_records>]                        

   Options:
            -p   --preview      in expression mode, show the generated lambda


filtr: command line utility for filtering record streams


""",

    "get-awssecret": """______________________________________________

+++  Mercury script: filtr +++
______________________________________________

Usage:
        get-aws-secret --region <region> --secret <secret_name> --tag <tag> --section <section>
        get-aws-secret --region <region> --secret <secret_name> --tag <tag> --sections
        get-aws-secret --region <region> --secret <secret_name> --tags

""",

    "ifthen": """______________________________________________

+++  Mercury script: filtr +++
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


### UNDER CONSTRUCTION ###


""",

    "jsonkey2lower": """______________________________________________

+++  Mercury script: jsonkey2lower +++
______________________________________________

Usage:
    jsonkey2lower --datafile <filename> [--limit <limit>]
    jsonkey2lower -s [--limit <limit>]

Options:
    -s --stdin  read data from standard input


jsonkey2lower processes a collection of JSON records (one per line), either from <filename>
if the --datafile option is set, or from standard input if the --stdin option is set,
and outputs a transformed recordset in which all the keys are lowercase.


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

+++  Mercury script: jsonLscan +++
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


jsonLscan reads a set of JSONL records (from <filename> if the --file option is set, or from 
standard input if the --stdin option is set) and returns a set of JSONL records. 

If the --list option is set, then each output record is a name/value pair where <name> 
is the actual <field> parameter, and <value> is source_record[<field>].

So processing a sample.json file containing

{"first_name": "John", "last_name": "Smith"}
{"first_name":"Bob", "last_name":"Barker"}

using the command:

jsonLscan -f sample.json --list first_name

yields the output:

{"first_name": "John"}
{"first_name": "Bob"}



Set the --raw parameter to get back only the values, as raw text. So that the command

jsonLscan -f sample.json --list first_name -r

will yield

John
Bob

(When the --prefix option is set, jsonLscan will prepend <prefix_string> to each raw output
record.)


jsonLscan is capable of indirection. When the --readfile option is set, it will scan records 
in <file> and read the value of the specified field, not as data, but as a reference to 
ANOTHER file. 

Suppose we have a sample.json file containing 

{"first_name": "John", "last_name": "Smith"}
{"first_name":"Bob", "last_name":"Barker"}

and we have a meta.json file containing

{"filename": "sample.json", "id": 123456}

-- then, if we issue the command

scripts/jsonLscan --file meta.json --readfile filename

jsonLscan will:

1. treat the value of the "filename" field in meta.json as an actual filename (which it is);
2. load the file which that field refers to (in this case. sample.json);
3. print each JSON record in the referenced file.

This will simply give us the contents of sample.json.


""",

    "jsonrec2csv": """______________________________________________

+++  Mercury script: jsonLscan +++
______________________________________________

Usage:
    jsonrec2csv --config <configfile> --setup <setup> --delimiter <delimiter> [--datafile <jsonfile>] [--limit=<max_records>]

""",

    "jtransform": """______________________________________________

+++  Mercury script: jsonLscan +++
______________________________________________

Usage:
    jtransform --config <configfile> --map <map_name> --datafile <jsonfile> 
    jtransform --config <configfile> --map <map_name> -s
    jtransform --config <configfile> --list

Options:
    -s --stdin  read JSON records from standard input

""",

    "loopr": """______________________________________________

+++  Mercury script: loopr +++
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


loopr: command-line utility for looping through lists and performing a user-defined action on each iteration.

In --text mode, loopr will use each line from the listfile as a monolithic value to be optionally inserted in the command string
wherever the --vartoken sequence is found (similar to the way xargs works with the -I argument).

In --json mode, loopr assumes that each line of the listfile is a JSON record, and will use th corresponding dictionary 
to populate any Python-style template variables in the command string.

In --csv mode, loopr will read the listfile as a set of CSV records and read each line into a dictionary which can be used 
to populate any Python-style template variables in the command string.

loopr will also run an optional command (specified by the --post-cmd argument) after each iteration.


""",

    "makeblocks": """______________________________________________

+++  Mercury script: makeblocks +++
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


makeblocks: command-line utility for managing data pipeline configurations based on the M2
(Mercury makefile) pattern.

A "Mercury" makefile is a canonical makefile, but organized and instrumented to run data pipelines.

An M2 data pipeline is a make target organized into a variable block and 1-N command blocks.
Variable and command blocks can be instrumented, with formatted comments, such that we can isolate
and inspect the blocks, load and inspect make variables, and resolve make-variable references
in command blocks.

Make targets can also be instrumented in this way, allowing us to step-debug data pipelines.

There are four types of logical structures we can define in the comments of an M2 makefile:
targetblocks, varblocks, cmdblocks, and checkpoints. A targetblock is defined at the top of
(immediately before) a make target with the syntax:

+open-targetblock

and must be closed at the end of the make target with:

+close-targetblock

(A targetblock is implicitly named; its name is always the name of the make target it encloses.)

A varblock is defined inside a make target and *explicitly* named, with

+open-varblock(blockname)
<make var declarations>
+close-varblock

A cmdblock is defined (also inside a make target) with

+open-cmdblock(blockname)
<1-N commands>
+close-varblock

A checkpoint may either be named, or anonymous. An anonymous checkpoint is defined with

+checkpoint:[var=<varblock_name>.<variable_name> test=<python_module>.<test_function>]

and a named checkpoint is defined with

+checkpoint:<checkpoint_name>[var=<varblock_name>.<variable_name> test=<python_module>.<test_function>]


""",

    "manifold": """______________________________________________

+++  Mercury script: manifold +++
______________________________________________

Usage:
    manifold [-d] --write --param-generator <module.generator_function> [--inputs=<name:value>...]
    manifold [-d] --read [-n] <paramfile> --warpcmd <command> --outfile-prefix <prefix> --outfile-suffix <suffix>
    manifold [-d] --read [-n] <paramfile> --warpfile <file> --outfile-prefix <prefix> --outfile-suffix <suffix>

Options:
    -n --noindex   Do not automatically generate index numbers for outfile names
    -d --debug     Run manifold in debug mode


manifold generates (or consumes) JSON records. 

In --write mode, manifold generates a list of JSON records by calling the user-defined logic in
<module.generator_function>. 

That function must follow the pattern:


def function_name(**kwargs) -> dict:
    ...


and must be a Pythonic generator which yields a dictionary. 

Any (comma-separated) name:value pairs passed to manifold as part of the --inputs option will be passed
to the generator function as keyword arguments.


In --read mode, manifold reads a list of JSON records (one per line) from <paramfile> and uses each record
to populate the warp template specified either by <command> (if --warpcmd is set) or <file> if --warpfile is set.

In --write mode, manifold sends its outputs directly to standard out. 

But in --read mode, because it generates an entire populated template per input-record, it will 
write its outputs to N files where N is the number of JSONL records it reads from <paramfile>.
Each output file is named according to the pattern:

<prefix>{generated number from 1 to N}<suffix>


""",

    "mapname": """______________________________________________

+++  Mercury script: manifold +++
______________________________________________

Usage:
    mapname <filename> --mapper-function <module.func> [--params=<name:value>...]


""",

    "mergein2j": """______________________________________________

+++  Mercury script: manifold +++
______________________________________________

Usage:
    mergein2j --from-csv <csvfile> --delimiter <delim> --column <colname> --into <jsonfile> [--limit=<limit>]
    mergein2j --from-csv <csvfile> --delimiter <delim> --colmap <mapfile> --into <jsonfile> [--limit=<limit>]
    mergein2j --from-csv <csvfile> --delimiter <delim> --keys=<key>... --into <jsonfile> [--limit=<limit>] 
    mergein2j --from-list <listfile> --key <key> --into <jsonfile> [--limit=<limit>]
    

""",

    "mergr": """______________________________________________

+++  Mercury script: mergr +++
______________________________________________

Usage:
    mergr --files=<filename>... --keys=<key>...



mergr: command-line utility for merging lists and emitting JSON records


Given file1.txt with content: 

one
two
three

and file2.txt with content:

blue
red
green

issuing the command:   

mergr --files=file1.txt,file2.txt --keys=number,color

would yield the JSON output:

{"number": "one", "color": "blue"}
{"number": "two", "color": "red"}
{"number": "three", "color": "green"}


""",

    "mkcfg": """______________________________________________

+++  Mercury script: mergr +++
______________________________________________

Usage:
  mkcfg --list
  mkcfg <target>
  mkcfg <target> --edit <configfile>

Options:
  -l --list       show the available configuration targets

""",

    "ngst": """______________________________________________

+++  Mercury script: mergr +++
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

+++  Mercury script: repeat +++
______________________________________________

Usage:
    repeat --count <num_times> [--str <string>]
    repeat --linecount <file> [--str <string>]


repeat emits the designated string some number of times.
if the --count parameter is used, it will repeat <num_times> times.

If the --linecount parameter is used, it will repeat as many times as there are lines in
<file>.


""",

    "seesv": """______________________________________________

+++  Mercury script: repeat +++
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

+++  Mercury script: repeat +++
______________________________________________

Usage:
    segmentr --config <configfile> --job-name <jobname> --jobfile <filename> --segments=<segname>,... [--segment-params=<n,v>...] [--limit=<max_records>]
    segmentr --config <configfile> --job-name <jobname> --jobfile <filename> --segment-count=<number> [--segment-params=<n,v>...] [--limit=<max_records>]


""",

    "sqs-consume": """______________________________________________

+++  Mercury script: sqs-consume +++
______________________________________________

Usage:
    sqs-consume --config <configfile> --source <source_name>
    sqs-consume --version


sqs-consume connects to an Amazon SQS queue, pulls messages down from that queue, and forwards
each message to a user-defined handler function.


""",

    "string2rec": """______________________________________________

+++  Mercury script: sqs-consume +++
______________________________________________

Usage:
    string2rec --str <string> --transformer <module.transform_func> [--params=<n,v>...]
    string2rec --listfile <filename> --transformer <module.function> [--params=<n,v>...] [--limit=<limit>]

""",

    "svctest": """______________________________________________

+++  Mercury script: sqs-consume +++
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

+++  Mercury script: xcombine +++
______________________________________________

Usage:
    xcombine --listfiles=<file1>... --delimiter <delim> [--working-dir <dir>]


xcombine (cross-combine) takes a number of lists (of equal length) as input, and returns 
a single list of CSV records representing all the combinations of the input fields.

So that given three input files:

file1.txt
A,B,C
1,2,3

file2.txt
D,E,F
4,5,6

file3.txt
G,H,I
7,8,9

and the command:

xcombine --listfiles file1.txt,file2.txt,file3.txt --delimiter ','

The output will be:

A,B,C,D,E,F,G,H,I
A,B,C,D,E,F,7,8,9
A,B,C,4,5,6,G,H,I
A,B,C,4,5,6,7,8,9
1,2,3,D,E,F,G,H,I
1,2,3,D,E,F,7,8,9
1,2,3,4,5,6,G,H,I
1,2,3,4,5,6,7,8,9


""",

    "xfile": """______________________________________________

+++  Mercury script: xfile +++
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


 xfile: command line utility for extracting and transforming CSV data


""",

    "xlseer": """______________________________________________

+++  Mercury script: xfile +++
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
