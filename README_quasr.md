##### QUASR (Quality Assurance SQL Runner)

**quasr** is a Python tool for writing automated data QA routines against relational databases. A quasr stack comprises one or more **jobs**, each of which consists of:

* a SQL query or query template
* user-defined  logic which executes the query and generates a structured-data report in JSON

quasr is a comand-line utility designed to maximize ease-of-use and minimize subtlety. Operators can use it to generate quantitative measures of dataset quality (fill-rates, null-value counts) and (optionally) apply user-defined heuristics to those measures.

**Prerequisites**:

* Python 3.5 or higher
* a UNIX (or clone) operating environment
* pip3 (or pipenv)

Getting Started

at the terminal, issue 

`pip install mercury-toolkit` 
or
`pipenv install mercury-toolkit`

after which you can issue `quasr` to generate a usage string.

###### Config File Structure
The configuration file for quasr consists of the following sections:

* globals
* service_objects
* templates (optional)
* jobs

The `globals` section contains the top-level settings `project_home` (from which quasr will load user-defined code modules); `qa_logic_module` (where user-defined Python functions are to be found); `template_module` (where SQL templates live), and `service_module` (where service object classes are defined.)

The `service_objects` config section contains references to service classes (long-running singletons loaded at application startup) in the project's specified service_module. Each entry is named dictionary under a top-level alias, by which application code can look up a live service object instance at runtime. The entry must contain a valid class name in the class parameter, and an array of name-value pairs under the init_params parameter.

The `templates` section contains named SQL template strings residing in the config file itself. Quasr templates may also reside in the `template_module` defined in the `globals` section.

The `jobs` section contains named QA jobs, any of which can be specified upon invocation of the `quasr` command. So if we issue

`quasr --config <configfile> --job <jobname>`

then `<jobname>` must match one of the top-level entries in the `jobs` section. Each entry contains:

* `sql_template` (an alias referencing an existing template)
* `inputs` (an array of entries each of which represents a variable field in the designated template)
* `outputs` (an array of entries which together represent a dictionary that we'll generate from our QA job)
* `executor_function` (the name of the user-defined Python function which will execute the SQL query)
* `builder_function` (the name of the user-defined Python function which will build the `outputs` dictionary from the query result)
* `analyzer_function` (the name of the optional user-defined Python function which will analyze our outputs 
and return a list of error-conditions and flag-conditions)

###### Creating a new QA stack
At the terminal, issue the command `mkcfg quasr`.

You will be greeted with the quasr command-line prompt from the mkcfg utility. You can type ? to explore the available commands, which allow you
to create, list, and edit the components of a quasr config -- but the fastest way to get started is to issue the `wizard` command and follow the
prompts. 

When filling out the globals section, you can use environment variables (for example, when specifying the project_home setting) by setting the first character of your entry to the $ character (no spaces). When quasr loads your configfile at runtime, it will try to load that variable from the shell
environment, and will exit with a warning if the variable does not exist.

The same is true of the service_objects section: when you specify a ServiceObject by name and class, then add init parameters, the value of any init parameter can be an environment variable. This is useful if you've written a ServiceObject requiring database credentials or other sensitive information which you want to avoid writing into a config file.

The wizard will guide you through the creation of all the required configuration sections. When you are done, you can preview the finished config using
the preview (or pre) command, then issue save to write it to a YAML file. 

Once you've generated a config file, you must verify that the references in that file (for example, to user-defined Python modules) are correct. Modules defined in the globals section, for example, must reside in the project_home directory (or somewhere on the PYTHONPATH). Functions defined in
a given job must actually exist in the designated global `qa_logic_module`. Template aliases referenced in a job must either be defined in the config file (surround the template alias with parentheses to refer to a template defined in the YAML file) or in the global template_module; service objects in the `service_module`, and so on. The mkcfg utility will not automatically check these conditions for you, as we expect that you may wish to create
a skeleton config first, then fill in the blanks later. In any case, given a valid config file, the quasr program will inform you of any missing references or un-findable modules or functions.

Issue quasr by itself at the terminal to generate a usage string.




