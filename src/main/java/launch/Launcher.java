package launch;

import evaluation.Balancing;
import evaluation.CutSize;
import evaluation.TimeMeasurement;
import io.FileUtils;
import io.GraphReader;
import io.GraphWriter;
import io.MetaWriter;
import model.Hypergraph;
import model.Partition;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import partitioning.PartitioningStrategy;
import partitioning.PartitioningStrategyFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class Launcher {


    private final static Logger logger = LogManager.getLogger(Launcher.class);


    public static void main(String[] args) throws ParseException {

        CommandLineParser parser = new DefaultParser();
        Options cliOptions = getCliOptions();

        // interrupt help call
        List<String> argList = Arrays.asList(args);
        if (argList.contains("--help") || argList.contains("-h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("MHP", cliOptions);
            System.out.println(PartitioningStrategyFactory.printHelp());
            // print strategy details
            System.exit(0);
        }

        CommandLine arguments = parser.parse(cliOptions, args);

        TimeMeasurement fileReadingTime = new TimeMeasurement();
        Hypergraph graph = null;


        // =====================
        // INTERROGATE ARGUMENTS
        // =====================

        GraphReader.InputFormat inputFormat = GraphReader.InputFormat.BIPARTITE;
        if (arguments.hasOption("format")) {
            String value = arguments.getOptionValue("format");
            switch (value.toLowerCase()) {
                case "edge_list":
                    inputFormat = GraphReader.InputFormat.EDGE_LIST;
                    break;
                case "bipartite":
                    inputFormat = GraphReader.InputFormat.BIPARTITE;
                    break;
                default:
                    logger.info("Input format not recognized. Using edge list.");
                    inputFormat = GraphReader.InputFormat.BIPARTITE;
                    break;

            }
        }

        Path inputFile = null;
        if (arguments.hasOption("inputFile")) {
            // read file
            try {
                fileReadingTime.startMeasurement();
                inputFile = Paths.get(arguments.getOptionValue("inputFile"));
                graph = GraphReader.readHypergraph(inputFile, inputFormat);
                fileReadingTime.endMeasurement();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            logger.info("Hypergraph loaded after " + fileReadingTime.getTimeDiferenceInSeconds() + " seconds.");
        }

        assert (inputFile != null);
        assert (graph != null);
        int graphSize = graph.getSize();
        int graphOrder = graph.getOrder();
        logger.info("Graph size (#edges): " + graphSize);
        logger.info("Graph order (#vertices): " + graphOrder);

        int numberOfPartitions = -1;
        if (arguments.hasOption("numberOfPartitions")) {
            numberOfPartitions = ((Number) arguments.getParsedOptionValue("numberOfPartitions")).intValue();
        }
        assert (numberOfPartitions > 1);

        PartitioningStrategy strategy = null;
        if (arguments.hasOption("strategy")) {
            strategy = PartitioningStrategyFactory.createStrategy(arguments.getOptionValues("strategy"));
        }
        assert (strategy != null);


        // ==================
        // GRAPH PARTITIONING
        // ==================

        logger.info("start partitioning....");
        TimeMeasurement partitioningTime = new TimeMeasurement();
        partitioningTime.startMeasurement();

        List<Partition> partitions = strategy.partition(graph, numberOfPartitions);
        partitioningTime.endMeasurement();

        logger.info("partitioning finished after " + partitioningTime.getTimeDiferenceInSeconds() + " seconds.");


        // =================
        // CALCULATE METRICS
        // =================

        // partition size
        logger.info("partition sizes: " + Arrays.toString(partitions.stream().mapToInt(Partition::getOrder).toArray()));

        MetaWriter metricsWriter = new MetaWriter(inputFile, strategy, partitions);
        metricsWriter.addMetric("partitioning time (ms)", String.valueOf(partitioningTime.getTimeDiferenceInMillis()));

        // graphSize is taken before the partitioning, since some algorithms (e.g. Hype) clear the graph during the partitioning
        long kMinus1CutSize = CutSize.calculateKMinus1Cut(partitions, graphSize);
        logger.info("k-1 cut: " + kMinus1CutSize);
        metricsWriter.addMetric("k-1 cut", String.valueOf(kMinus1CutSize));

        double maxImbalance = Balancing.calculateMaxImbalance(partitions);
        logger.info("max imbalance: " + maxImbalance);
        metricsWriter.addMetric("max imbalance", String.valueOf(maxImbalance));

        double balancingDeviation = Balancing.calculateStandardDeviationFromBalance(partitions);
        logger.info("balancingDeviation: " + balancingDeviation);
        metricsWriter.addMetric("balancingDeviation", String.valueOf(balancingDeviation));

        // ===========
        // FILE OUTPUT
        // ===========
        
        metricsWriter.writeMetaData();

        if (arguments.hasOption("output")) {
            logger.info("writing output files...");
            GraphWriter.write(Paths.get(inputFile.toString().replace(FileUtils.getLastSegmentOfPath(inputFile), "")), strategy.getStrategyName(), partitions);
        }

        logger.info("MHP finished.");
    }


    /**
     * creates an options object for command line arguments with apache commons cli
     *
     * @return the command line argument options object
     */
    private static Options getCliOptions() {
        Options options = new Options();

        // command line arguments
        // INPUT FILE
        Option inputFile = Option.builder("i").longOpt("inputFile").argName("Input File").desc("The input file containing a hypergraph")
                .hasArg().required().type(String.class).build();
        options.addOption(inputFile);

        // NUMBER OF PARTITIONS
        Option numberOfPartitions = Option.builder("n").longOpt("numberOfPartitions").argName("Number of Partitions").desc("The number of subgraphs to be created")
                .hasArg().required().type(Number.class).build();
        options.addOption(numberOfPartitions);

        // PARTITIONING STRATEGY
        Option strategy = Option.builder("s").longOpt("strategy").argName("partitioning strategy").desc("specifies which strategy to use. See below for details")
                .numberOfArgs(Option.UNLIMITED_VALUES).required().type(String.class).build();
        options.addOption(strategy);

        // INPUT FORMAT
        Option inputFormat = Option.builder("f").longOpt("format").argName("input format").desc("format of the input file, e.g. bipartite")
                .hasArg().type(String.class).build();
        options.addOption(inputFormat);

        // OUTPUT
        Option resultOutputRequired = Option.builder("o").longOpt("output").argName("Result Output").desc("if set, the partitioning result is written to the hdd")
                .hasArg(false).build();
        options.addOption(resultOutputRequired);

        // HELP
        Option help = Option.builder("h").longOpt("help").argName("Help").desc("print this Message").build();
        options.addOption(help);

        return options;
    }
}
