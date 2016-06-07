# Elasticsearch-Exporter Developers Guide
This is a guide for developers looking to implement their own drivers for use with the exporter v2.0. In addition to looking at existing drivers or the driver
interface in the drivers directory, this will hopefully make it easy for you to implement your own driver.


## Introduction
Every function (except for the optional functions) has a callback function where the first parameter is an error object. You can use this to send back errors
to the driver in form of a single string, as an array of messages or even as an Error object. The exporter is configured to automatically retry on error so that
the same call can happen several times. If you wish to stop execution at a certain point without retrying, simply don't call the callback function and the
exporter will terminate with your function. Alternatively you can also throw an Error at any point or kill the script manually.

For logging purposes you can of course use the console.log function, but it is encouraged to make use of the log.js tools, which include different log levels
(debug, info and error) which follow different verbosity levels of the logging options. The log tools also include a die() function that will stop execution of
the exporter with a nicely printed message, which you use instead of a direct process.kill().

### Driver verification
Each driver is verified during the loading process to see if at least the required functions and parameters are used. Unfortunately this requires you to use standard
parameter names for all required parameters, but helps in keeping a globally recognizable standard in code. Of course you can add any number of additional parameters
to your functions for when you call them yourself. When running the exporter with your driver included it will tell if any errors have been found.

### Loading drivers
For the exporter to find your driver and initialize it 2 requirements need to be fulfilled:
1. The driver must have the extension ```.driver.js```
2. The driver must be in one of the driver directories
The standard driver directory is called ```./drivers```, but using the ```--drivers.dir``` option you can specify additional directories that will be scanned for
drivers.


## Phases
The driver will be going through a number of phases and depending on whether you're the source or the target driver you'll go through different phases. Each
phase will only be executed once, except for the get data and put data calls which will be called until all data has been exported. An exception to this rule is
if you're driver reports and error and retries have been configured. Each call has his own retry counter, so that errors earlier will not be counted for later
phases. Note though that calls to getInfo(), verifyOptions() and end() are not repeated and only performed once.

The exporter uses the ElasticSearch data format internally, which you can always look up in the
[online documentation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/index.html). Since it's a very flexible json document that supports
storing custom meta data it should be good enough for all your needs. Storing custom metadata is especially then useful when you end up importing and exporting
from the same driver. For more information see the respective phases.

### 1. Get info  ( getInfo() )
This function receives no other parameters then the callback on which to return errors, the info object and the options object.
```callback(error, info, options)``` The info object holds general information about the driver that will be used to match it and determine it's capabilities.
The format is as follows:

```JavaScript
{
    id: 'your driver unique id',
    name: 'your driver name',
    version: '1.0',
    description: 'a description of what your driver does'
}
```

The options object holds all options that your driver accepts and parse the from the command line. The object allows you set preset/default values and mark options
as required. The options object is separated into two sections: source and target, which each have independent settings object. If an option is available for both
modes, you will have to define the option twice.

```JavaScript
{
    source: {
        myoption: {
            abbr: 'a required short version of the option',
            preset: 'the default value if none given at command line',
            help: 'this will be printed in the help section if this driver is selected',
            flag: 'Means that no value is expected and this will be either true or false'
        }
    },
    target: {
        example: {
            abbr: 'e',
            preset: true,
            help: 'This is a target option that is defaulting to true'
            flag: true
        }
    }
}
```

### 2. Verify options ( verifyOptions(opts) )
The verify method receives the parsed and evaluated options for this driver. If the the driver is both the source and the target, this method is still only
called once. Here you have the chance to perform additional verification on the parsed options, e.g. if combinations of options are valid. It also allows you
to set additional options that can be inferred after having access to available options.

If there are any errors you can specify them with the callback ```callback(error)```, otherwise just use an empty callback. In general you can access options
in the options object, which is part of the environment object in this way: ```options.source.myoption``` or ```options.target.example```. For mor information
on the environment object see below.

### 3. Reset Source ( reset(env) )
This function is only called on the source driver and is meant to prepare the driver and set it into a ready mode. Typically this function is not called again
in a normal execution, but if the exporter is used as a library it could very well be that several runs are performed with one instance of a driver, it is
therefor good practice to make sure a reset will work at any time, not only before the first run. Again if there is an error you can report it using the
callback function.

### 4. Reset Target ( reset(env) )
This acts the same way as the reset source call and uses the same function name, but for the target driver. Note that both functions will be called even if the
same driver is used, so if you're both source and target, your function will be called twice. You can make use of the environment object to check whether the
source, target, or both drivers match you id.

### 5. Get source statistics ( getSourceStats(env) )
This function is called to the source and is another way of checking if the database is actually ready. While it is possible that some requests to the database
have been made in previous calls, this is the method in which an outgoing call is definitely expected. The callback should include an object holding all relevant
statistics that can be used by the target driver and the exporter itself to determine the best way to process incoming data ```callback(error, statistics).
The stats document requires only these properties for the exporter to run, any additional statistics are for the target driver:

```JavaScript
{
    status: "Green",
    docs: {
        total: 123
    }
}
```
__statistics.status__ is required to determine if the source database is alive. Any value other than "red" will keep the exporter going.

__statistics.docs.total__ is required to determine how many documents will be exported. This will most likely require you to run a count query before the
actual export starts. It is important that this number is acurate as it decides when the script will terminate.

Any additional statistics can be added by you which will be available for the target driver. A common value that is passed on e.g. is the version number of the
database, which can prompt some conversion operations in the target driver. At the end of the execution of the exporter all statistics will be printed as a final
status message.

### 6. Get target statistics ( getTargetStats(env) )
Same as source statistics only that no number of documents property is required.

### 7. Get meta data ( getMeta(env) )
This function is called to retrieve any meta data (or table definitions or whatever your database uses to define a schema) from the source database. As mentioned
previously this follows the ElasticSearch format here's an example of what a mapping for a single type on elasticsearch could look like:

```JavaScript
{
  "myType": {
    "_id": {"index": "not_analyzed"},
    "_parent": {"type": "myParentType"},
    "_timestamp": {"enabled": true},
    "_ttl": {"enabled": true},
    "_meta": {
      "metaProp": "metaVal"
    },
    "properties": {
      "myProperty": {
        "type": "string",
        "store": true,
        "index": "analyzed",
        "null_value": "na",
        "boost": 2.0
      }
    }
  }
}
```

Of course there are many more options and fields available that you can make use of. For a reference check ElasticSearch's
[mapping documentation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping.html). Note that you can also add extra properties to the
mapping even if they are not supported by standard ElasticSearch features by using the _meta field. This could allow you to store information about the source
that will allow you later to rebuild the information in the original database for example. When done the callback should return the mappings for all types that
are beign exported: ```callback (error, mapping)```.

### 8. Store meta data ( putMeta(env, metadata) )
This is called once the metadata has been retrieved from the source database and allows you to set up your mapping, tables or other schema definitions. As usual
if there are any errors you can report them with the callback.

### 9. Prepare transfer ( prepareTransfer(env) ) (optional)
This is an optional step that has been introduced to make it easier to set up a data transfer when run as a multi process driver. If your driver is capable of
running in multi threaded mode, all your previous code executions will be ignored since we're starting a completely new process for the rest of the functions.
Some database still need to establish a connection, open a file pointer, etc. to prepare reading/writing data. Use this function to make it easier to set this up.
Note though that there is no callback on the function so operation will need to be synchronized or the next step for you driver will have to check and wait for
the driver to be ready. For more details on multi process/threaded execution see below.

### 10. Get data ( getData(env, callback, from (optional), size (optional)) )
This is the first function that will be called repeatedly until all data has been fetched. The function accepts two optional parameters that let the driver know
where we are in the processing queue and how many documents are expected to be returned. It is critical that the size parameter is followed, as this decides if
the script will terminate at some point or keep hammering the source database for more data. Returning fewer documents than the size will lead to missing documents
during the export.

The data format follows again the standard ElasticSearch format which lists them in an array. En Example document with the minimum required fields would look like
this:

```JavaScript
{
    "_index" : "sourceIndex",
    "_type" : "sourceType",
    "_source" : {
        "property" : "value",
        "message" : "trying out the Exporter"
    }
}
```

Of course there are quire a few more properties that are supported by ElasticSearch, such as specifying your own IDs and timestamps .For more details on what
additional fields can be stored check out the [ElasticSearch reference](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html).
The callback accepts the data array and uses the length of it to verify if enough data has been returned.
callback (error, data)

### 11. Store data ( putData(env, data) )
This function should store the received data in the target database. The format matches the specification of getData(). Use the callback to signal any errors.

### 12. End (optional)
If the driver specifies an end function, this will be called when all operations have terminated. This will allow the driver to shut down gracefully if required.
As with the prepareTransfer() function this will be called once for each worker, or only once if the driver doesn't support multi process/thread execution.


## The environment object
The enironment object is passed to almost all functions and works as a state holder of the current export going on. The object has two main properties: the options
and the statistics. A few standard properties are available and expected to exist which you can see in this example:

```JavaScript
{
    options: {
        drivers: {
            source: 'source ID',
            target: 'target ID'
        },
        source: {
            myoption: 'value'
        },
        target: {
            example: true
        }
    },
    statistics: {
        source: {
            version: "0.0",
            status: "Green",
            docs: {
                total: 100
            }
        },
        target: {
            version: "0.0",
            status: "Green"
        },
        hits: {
            fetched: 0,
            processed: 0,
            total: 0
        },
        memory: {
            peak: 0,
            ratio: 0
        }
    }
}
```

The options property holds the data parsed from command line after being processed and verified by the driver. Access to all options is available, even the driver
specific options. Note that you can check what driver the source or target is in the ```options.drivers``` object.

The statistics object is a composite of the source and target statistics of the drivers fetches from getSourceStas(), getTargetStats() and stats set by the
exporter.


## Multithreaded processing
TBD.

* Only runs in threaded mode if source and target drivers support it
* Probably doesnt't work with scanning type readers (but might if you separate into separate scanning processes per worker)
* Env object is only copied
* workers don't share anything
* workers MUST respond with the number of documents specified in "step"


## Getting help
Same as with the exporter itself, I will always try and help fellow developers to get things working, so if you can't get any further check the issues at
[Github](https://github.com/mallocator/Elasticsearch-Exporter/issues) and file one if there isn't already a solution for you. You can also reach me directly at
mallox@pyxzl.net if that's your preferred way of communication.
