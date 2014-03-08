package io.keen.sample.java;


import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.keen.client.java.JavaKeenClient;
import io.keen.client.java.KeenClient;
import io.keen.client.java.KeenLogging;
import io.keen.client.java.KeenProject;

/**
 * Main class for the sample application.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 */
public class Main {

    ///// PUBLIC STATIC METHODS /////

    /**
     * Executes the application with the given arguments.
     *
     * @param args The arguments.
     */
    public static void main(String[] args) throws Exception {
        // Parse the command line arguments.
        Options options = buildCommandLineOptions();
        CommandLine cmd = parseCommandLine(args, options);

        // If help was requested, just display it and exit.
        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("keen-sample [OPTION]...", options);
            System.exit(0);
        }

        // Initialize the Java Keen client.
        KeenClient client = JavaKeenClient.initialize();

        // If logging was specified, enable it.
        if (cmd.hasOption("logging")) {
            KeenLogging.enableLogging();
        }

        // If debug mode was specified, enable it.
        if (cmd.hasOption("debug")) {
            client.setDebugMode(true);
        }

        // Get the number of events to send from the arguments, or use the default.
        int numEvents = DEFAULT_NUM_EVENTS;
        if (cmd.hasOption("num-events")) {
            numEvents = Integer.parseInt(cmd.getOptionValue("num-events"));
        }

        // Get the number of threads to use to send events.
        int numThreads = DEFAULT_NUM_THREADS;
        if (cmd.hasOption("num-threads")) {
            numThreads = Integer.parseInt(cmd.getOptionValue("num-threads"));
        }

        // Get properties defining how to send the events.
        boolean synchronous = !(cmd.hasOption("async"));
        boolean batch = cmd.hasOption("batch");

        // Construct an instance of the Main class.
        Main main = new Main(client, synchronous, batch, numEvents, numThreads);

        // Set the default project based on system properties.
        main.setDefaultProject(System.getProperties());

        // Execute the program.
        main.execute();
    }

    ///// PUBLIC METHODS //////

    public void execute() throws InterruptedException {
        // Print out the parameters of this execution.
        System.out.printf(
                Locale.US, "Sending %d events from %d threads using %s requests and %sbatching\n",
                numEvents, numThreads, (synchronous? "synchronous" : "asynchronous"),
                (batch? "" : "no "));

        // Seed the random number generator.
        long seed = System.currentTimeMillis();
        rng.setSeed(seed);
        System.out.println("Random seed: " + seed);

        // Build a set of tasks to run.
        List<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
        for (int i = 0; i < numThreads; i++) {
            Callable<Integer> eventAdderTask = new EventAdder();
            tasks.add(eventAdderTask);
        }

        System.out.println("Starting workers in thread pool...");

        // Create a thread pool to run the tasks, run them, and then shutdown the pool.
        ExecutorService workers = Executors.newFixedThreadPool(numThreads);
        List<Future<Integer>> futures = workers.invokeAll(tasks);

        System.out.println("Workers starters; awaiting completion");

        for (Future<Integer> future : futures) {
            try {
                Integer eventCount = future.get();
                System.out.printf("Worker finished after adding %d events\n", eventCount);
            } catch (Exception e) {
                System.err.println("Worker exited with exception: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }

        // If this execution used batching, perform one final synchronous upload to catch any
        // events that may have been added after the last upload due to thread scheduling.
        if (batch) {
            System.out.println("Performing final clean-up sendQueuedEvents");
            client.sendQueuedEvents();
        }

        System.out.println("All workers have exited; shutting down worker ExecutorService");

        workers.shutdown();
        workers.awaitTermination(60, TimeUnit.SECONDS);

        System.out.println("Worker ExecutorService shut down; shutting down publish " +
                "ExecutorService");

        // Shutdown the publish executor service for the Keen client.
        ((JavaKeenClient) client).shutdownPublishExecutorService(60 * 1000);

        // Print the expected sum of the "counter" column, which can be used to validate that the
        // Keen server received all of the events.
        System.out.println("Expected sum of counters: " + getExpectedSum());
        System.out.println("You can verify this sum via the web workbench, or " +
                "with the following curl command:");
        KeenProject defaultProject = client.getDefaultProject();
        System.out.printf("  curl \"https://api.keen.io/3.0/projects/%s/queries/sum?" +
                "api_key=%s&event_collection=sample-app&target_property=counter\"",
                defaultProject.getProjectId(), defaultProject.getWriteKey());
    }

    public void setDefaultProject(Properties properties) {
        String projectId = properties.getProperty("io.keen.project.id");
        String writeKey = properties.getProperty("io.keen.project.write_key");
        String readKey = properties.getProperty("io.keen.project.read_key");
        KeenProject project = new KeenProject(projectId, writeKey, readKey);
        client.setDefaultProject(project);
    }

    ///// PRIVATE TYPES /////

    private class EventAdder implements Callable<Integer> {

        @Override
        public Integer call() throws Exception {
            int numEventsAdded = 0;
            int nextEventCounter;
            while ((nextEventCounter = counter.getAndIncrement()) <= numEvents) {
                addEvent(nextEventCounter);
                numEventsAdded++;
                Thread.sleep((long) rng.nextInt(MAX_SLEEP_INTERVAL_MILLIS));
            }
            return numEventsAdded;
        }

    }

    ///// PRIVATE CONSTANTS /////

    private static final String COLLECTION_NAME = "sample-app";
    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final int DEFAULT_NUM_EVENTS = 5;
    private static final int DEFAULT_NUM_THREADS = 1;
    private static final int MAX_SLEEP_INTERVAL_MILLIS = 50;

    ///// PRIVATE FIELDS /////

    private final KeenClient client;
    private final boolean synchronous;
    private final boolean batch;
    private final int numEvents;
    private final int numThreads;
    private final AtomicInteger counter;
    // TODO: Make seed configurable.
    private final Random rng;

    ///// PRIVATE CONSTRUCTORS /////

    private Main(KeenClient client, boolean synchronous, boolean batch, int numEvents, int numThreads) {
        this.client = client;
        this.synchronous = synchronous;
        this.batch = batch;
        this.numEvents = numEvents;
        this.numThreads = numThreads;
        counter = new AtomicInteger(1);
        rng = new Random();
    }

    ///// PRIVATE STATIC METHODS /////

    private static Options buildCommandLineOptions() {
        Option help = new Option("h", "help", false, "Print this message");
        Option async = new Option("a", "async", false, "Use asynchronous methods");
        Option batch = new Option("b", "batch", false, "Use queueing and batch posting");
        Option numEvents = OptionBuilder.withLongOpt("num-events")
                .withDescription("Number of events to post")
                .hasArg()
                .withArgName("COUNT")
                .create();
        Option numThreads = OptionBuilder.withLongOpt("num-threads")
                .withDescription("Number of threads from which to post events")
                .hasArg()
                .withArgName("COUNT")
                .create();
        Option logging = new Option("l", "logging", false, "Enable Keen logging");
        Option debug = new Option("d", "debug", false, "Enable debug mode");

        // Add all of the options to an Objects object and return it.
        Options options = new Options();
        options.addOption(help);
        options.addOption(async);
        options.addOption(batch);
        options.addOption(numEvents);
        options.addOption(numThreads);
        options.addOption(logging);
        options.addOption(debug);
        return options;
    }

    private static CommandLine parseCommandLine(String[] args,
                                                Options options) throws ParseException {
        CommandLineParser parser = new BasicParser();
        return parser.parse(options, args, true);
    }

    private static int sumOfFirstNIntegers(int n) {
        return (n * (n + 1)) / 2;
    }

    private static String generateString(Random rng, int minLength, int maxLength)
    {
        int length = rng.nextInt(maxLength - minLength) + minLength;
        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            // Generate a random ASCII character between space (0x20) and ~ (0x7E).
            text[i] = (char) (' ' + rng.nextInt('~' - ' '));
        }
        return new String(text);
    }

    ///// PRIVATE METHODS /////

    private void addEvent(int n) {
        Map<String, Object> event = buildEvent(n);

        if (batch) {
            // Queue the event. This is always done synchronously.
            client.queueEvent(COLLECTION_NAME, event);

            // Periodically issue a batch post.
            if (n % DEFAULT_BATCH_SIZE == 0) {
                if (synchronous) {
                    client.sendQueuedEvents();
                } else {
                    client.sendQueuedEventsAsync();
                }
            }
        } else {
            if (synchronous) {
                client.addEvent(COLLECTION_NAME, event);
            } else {
                client.addEventAsync(COLLECTION_NAME, event);
            }
        }
    }

    private Map<String, Object> buildEvent(int n) {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("counter", n);
        event.put("string", generateString(rng, 3, 20));
        return event;
    }

    private int getExpectedSum() {
        return sumOfFirstNIntegers(numEvents);
    }

}
