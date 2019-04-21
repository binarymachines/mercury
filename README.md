# mercury
Python toolset for data management and pipeline construction

Mercury is a data engineering toolkit. It is built according to the UNIX philosophy; that is, it's composed of a number of relatively simple
programs, each of which:

- is small
- does one thing well
- is designed to be composable with other programs
- is easy to test

The design objective is to allow operators to create ETL (Extract, Transform, and Load) stacks quickly and with relative ease.

## TOOLING

## xfile
`xfile` reads a series of source records, either from an input file or from stdin, applies a transform to each record, and writes the transformed
record to stdout. It is driven by a YAML configuration file. 

`xfile` employs a configurable type called a Map, along with a pluggable type called a DataSource. A Map is a structured type describing the relationship between an input record (a set of name-value pairs) and an output record.

The configuration file specifies an arbitrary number of named Maps. Each map contains some metadata, a reference to a DataSource, and a collection of fields. The fields comprise a nested dictionary inside the map where the key is the output field name. Within each field we can have an input field name and a source for that field; that is, the place we go to find the field's value during a mapping operation. 

So for example, if we wished to map an input field called `"FIRSTNAME"` to an output field called `"first_name"`, we would add to our YAML file a field called "first_name" and specify its source as 
the record itself (meaning the inbound record) and its key as "FIRSTNAME". The `xfile` script, based on this configuration, will build
the mapping logic to generate an output record with the correct field name and the correct value:

```yaml
    fields:        
        - first_name:
            source: record
            key: FIRSTNAME
```

In the case of derived or computed field values, we first write a DataSource class and register it in our YAML config. 
We do this by updating the python module name specified in the lookup_source_module setting, in the `globals` section of our configfile... 

```yaml  
globals:
    project_home: $PROJECT_HOME
    lookup_source_module: test_datasources # <-- this module must be on the PYTHONPATH
    service_module: test_services
```


...and adding an entry in the top-level `sources` section.

```yaml
  sources:
    eosdata:
      class: TestDatasource  # <-- this class must exist in the test_datasources module 
```

finally we let our map know that its designated datasource is the one we just specified:

```yaml
  maps:
    default:
      settings:
          - name: use_default_identity_transform
            value: True

      lookup_source: 
        testdata # <-- this must match the name under which we registered the TestDatasource class
``` 
      
 Now, in the individual field specs of our map, we can specify that a given output field will get its value not from the
 input record, but from the datasource:
 
```yaml
  fields:
      - calculated_field_name:
            source: lookup
```     
      
And `xfile`, on startup, will dynamically load our Datasource and verify that it exposes a method called 
 `lookup_calculated_field_name(...)`, calling that method when it needs to generate the mapped field value. In cases where the name of an output field is not compatible with Python function naming rules, we can specify the lookup function's name explicitly:
 
 ```yaml
  fields:
      - <calculated field name with spaces>:
            source: lookup
            key: <arbitrary_function_name>
```  
 
 
Note that a single YAML initfile can have multiple maps and multiple DataSources, so that we can select arbitrary mappings simply by specifying the desired map as a command line argument.
 
We run `xfile` by specifying a configuration file, an optional delimiter (the default is a comma), the name of a registered map from the config, and the source CSV file:

`xfile --config <yaml file> --delimiter $'\t' --map <map_name> input.csv`

There is also an optional `--limit` setting for testing `xfile` against very large input datasets:

`xfile --config <yaml file> --delimiter $'\t' --map <map_name> --limit 5`  <-- only transform the first five records from source 
 
There is also an `-s` option that allows `xfile` to stream records from standard input rather than read them from a file. This is useful when we wish to construct pipeline stacks at the command line by piping the output of a "csv emitter" process to `xfile`. 

`xfile` currently accepts either CSV or JSON data (one record per input line) and outputs JSON records only.

## ngst
The `ngst` script reads input records (such as those emitted by `xfile`) from a file or stdin, and writes them to a specified IngestTarget, which is simply a named destination for output records. Like `xfile`, it's driven by a YAML configuration file.

Each ingest target in the configfile consists of a reference to: 

- a dynamically loaded `DataStore` class
- a `checkpoint_interval` setting which selects the buffer depth in number of records (a checkpoint interval of 1 selects unbuffered writes to the target). 

Operators can simply subclass the DataStore base class and implement the `write()` method, then register the plugin in the YAML file under the top-level `datastores` key:
 
 ```yaml
   datastores:
      file:
          class: FileStore
          init_params:
                  - name: filename
                    value: output.txt
```

then, at the command line, specify the target by key. `ngst` will read the input records and write the output records to the designated DataStore.

`ngst --config <configfile> --target file --datafile my_json_records.txt`

Provided the write() method of the FileStore class fulfills its implicit promise (by writing records to the specified file), the above command string will write the records in `my_json_records.txt` to the file `output.txt`.

This is a trivial example, but it is easy to drop in different functionality (for example, writing records to a database) simply by writing a new datastore class and referring to it in the config.

What this means is that we can craft a very simple command line to perform extract, transform, and load -- just by invoking `xfile` and piping its output to `ngst`, where `xfile` uses a map with the desired transform and `ngst` uses a datastore pointing at the target storage layer.

If our ETL process is more complex -- for example, if we wish to first write extracted records to a producer-consumer queue for later consumption -- that is possible as well; we would simply write a DataStore class that is a queue producer, then execute the same command line and pass a different `--target` parameter to `ngst`.


### note: ServiceObjects
A ServiceObject is simply a long-running singleton which is spun up at program start. It is exactly the same type as is used in the 
Binarymachines SNAP (Small Network Applications in Python) microservices library, and it is initialized in exactly the same way
(the YAML config syntax is drop-in compatible). By using ServiceObjects, we can easily inject complex dependencies such as AWS or GCP targets, standalone databases, or any other services required by `xfile` or `ngst`. 

## j2sqlgen
`j2sqlgen` reads a JSON file representing RDBMS table metadata, follows the instructions declared in an YAML config file, and emits a CREATE TABLE statement suitable for direct execution against a target database. This is useful when we need to create data mobility pipelines from one database to another and when the column datatypes in the source and target databases are not compatible. This is the case, for example, when we are reading records from BiqQuery into Redshift; what BigQuery calls a STRING column, Redshift calls a VARCHAR(<size>). Rather than hand-craft SQL, we can use `j2sqlgen` to autogenerate the correct CREATE TABLE sql syntax. 

j2sqlgen lets us specify default behaviors for:
- autocreation of primary key columns of a specified type
- mapping of source to target column names
- default length of VARCHAR types
- mapping of source to target column types

and override all of these settings (except for the last) on a per-table basis. `j2sqlgen` emits cleanly to standard out, and so can be piped directly to any command (such as `psql`) which is capable of reading SQL. 

Take the example of the following BigQuery source table:

[TODO: source DDL]

we can generate a JSON representation of that table's metadata, which would look like this:

[TODO: JSON metadata]

then craft a configfile like this:

[TODO: YAML config]


and finally invoke j2sqlgen as follows:

`j2sqlgen --config <my_config> --source <json_schema_file> --table <tablename>`

and we will receive correctly-formatted SQL on stdout, capable of being executed against the target database.

If we want to create tables in a target schema with a different name from the source schema, we can pass that as an argument:

`j2sqlgen --config <my_config> --source <json_schema_file> --schema <target_schema> --table <tablename>`




