Sample Java App Using Keen
=============================

This is a sample Java app that uses the [Keen IO Java SDK](https://github.com/keenlabs/KeenClient-Java) to capture and upload events to Keen IO.

## Getting Started

1. `git clone git@github.com:keenlabs/KeenClient-Java-Sample.git`
1. `cd KeenClient-Java-Sample`
1. `export JAVA_HOME=<path to Java>` (Windows: `set JAVA_HOME=<path to Java>`)
1. `./gradlew build` (Windows: `gradlew.bat build`)


## Running the App

Once you've built the sample app, you can execute it just like any other java jar:

    java -jar build/libs/KeenClient-Java-Sample-<version>.jar --help

However, this won't work unless you also set up the classpath to include all of the necessary dependencies. Fortunately, Gradle can do this for you.

### Running From Gradle

The Gradle "run" task (provided by the "application" plugin) will automatically handle fetching dependencies and setting up the classpath:

```
% ./gradlew run -Pargs="--help"
:compileJava UP-TO-DATE
:processResources UP-TO-DATE
:classes UP-TO-DATE
:run
usage: keen-sample [OPTION]...
 -a,--async                 Use asynchronous methods
 -b,--batch                 Use queueing and batch posting
 -d,--debug                 Enable debug mode
 -h,--help                  Print this message
 -l,--logging               Enable Keen logging
    --num-events <COUNT>    Number of events to post
    --num-threads <COUNT>   Number of threads from which to post events
```

The help message shows some of the available options. Note that you need to provide a project ID and write key in system properties. Also, in order to pass command-line arguments through Gradle, a little magic is required. A typical execution might look something like this:

    ./gradlew run \
      -Dio.keen.project.id=<project ID> \
      -Dio.keen.project.write_key=<write key> \
      -Pargs="-b -d -l --num-events 100 --num-threads 3" \
      > output.log

Note that if you use the `-l/--logging` parameter the SDK will print every request and response to STDOUT, so you may want to redirect to a file.

## Code Explanation

The sample app is fairly straightforward. Besides command-line parsing, most of the work is done in two methods:

* `public void execute() throws InterruptedException` - This method sets up the necessary worker threads, starts them, waits for them to finish, and then cleans up.
* `private void addEvent(int n)` - This method generates an event with a monotonically increasing "counter" and a random "string", and then either posts the event to the server or queues it depending on whether the `--batch` flag was specified.
  * The presence of absence of the `--async` flag determines whether synchronous or asynchronous SDK methods are used.
  * If batching, then every 10 events a batch post will be sent to the server.

The threading logic ensures that each value of the counter is used exactly once. This provides a handy way to verify that all of your events were sent to the server: compute the sum of the first `NUM_EVENTS` integers and query the API for the sum of the `counter` property. The sample app will print a curl command which can be used to issue the query, as well as the expected sum.