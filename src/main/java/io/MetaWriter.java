package io;

import evaluation.TimeMeasurement;
import model.Hypergraph;
import model.Partition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import partitioning.PartitioningStrategy;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Writer to write the meta data of a partitioning to the hdd
 */
public class MetaWriter {

    private final static Logger logger = LogManager.getLogger(MetaWriter.class);

    private final PartitioningStrategy strategy;
    private final Path graphFile;
    private final List<Partition> partitions;
    private ArrayList<MetricTuple> metrics;

    public MetaWriter(Path graphFile, PartitioningStrategy strategy, List<Partition> partitions) {
        this.graphFile = graphFile;
        this.metrics = new ArrayList<>();
        this.strategy = strategy;
        this.partitions = partitions;

    }

    /**
     * adds a metric tuple to the metric set
     * @param name name of the metric
     * @param value value of the metric
     */
    public void addMetric(String name, String value) {
        metrics.add(new MetricTuple(name, value));
    }

    /**
     * writes all added metrics to the HDD
     */
    public void writeMetaData() {
        try {
            Instant timeStamp = TimeMeasurement.getCurrentTimeStamp();
            // create file
            Path metricsPath = Paths.get(Paths.get(graphFile.toString().replace(FileUtils.getLastSegmentOfPath(graphFile), "")).toString(),
                    String.format("%s_%s_%d.metrics", FileUtils.getLastSegmentOfPath(graphFile), strategy.getStrategyName(), timeStamp.getEpochSecond()));

            if (!metricsPath.toFile().createNewFile()) {
                logger.info("File already exists: " + metricsPath.toString());
            }

            // write via buffered writer
            BufferedWriter bw = Files.newBufferedWriter(metricsPath);

            bw.write(FileUtils.getLastSegmentOfPath(graphFile));
            bw.newLine();

            bw.write(strategy.getParameters());
            bw.newLine();

            bw.write(timeStamp.toString());
            bw.newLine();


            // partitions
            bw.newLine();

            bw.write("#partitions: " + partitions.size());
            bw.newLine();

            bw.write("partition size (edges): [" + partitions.stream().map(Hypergraph::getSize).map(Object::toString).collect(Collectors.joining(",")) + "]");
            bw.newLine();

            bw.write("partition order (vertices): [" + partitions.stream().map(Hypergraph::getOrder).map(Object::toString).collect(Collectors.joining(",")) + "]");
            bw.newLine();


            // metrics
            bw.newLine();

            for (MetricTuple tuple: metrics) {
                bw.write(tuple.name + ": " + tuple.value);
                bw.newLine();
            }


            bw.flush();
            bw.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static class MetricTuple {
        String name;
        String value;

        public MetricTuple(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
}
