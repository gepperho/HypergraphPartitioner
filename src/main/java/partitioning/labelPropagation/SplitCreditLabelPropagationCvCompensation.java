package partitioning.labelPropagation;

import gnu.trove.map.hash.TIntObjectHashMap;
import model.Hypergraph;
import model.Partition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class SplitCreditLabelPropagationCvCompensation extends SplitCreditLabelPropagation {

    private final static Logger logger = LogManager.getLogger(SplitCreditLabelPropagationCvCompensation.class);

    private int rebalancingRate;

    public SplitCreditLabelPropagationCvCompensation() {
        super(0);

        this.rebalancingRate = 3;
        iterations = 3 * rebalancingRate + 2;
        this.balancing_e_exponent = 4;
    }

    @Override
    public List<Partition> partition(Hypergraph graph, int numberOfPartitions) {

        this.graph = graph;
        partitionCounter = new AtomicIntegerArray(numberOfPartitions);
        this.numberOfPartitions = numberOfPartitions;

        for (int i = 0; i < this.numberOfPartitions; i++) {
            partitionCounter.set(i, 1);
        }


        // initialize seeds
        initializeSeeds(numberOfPartitions);


        for (int iteration = 0; iteration < iterations; iteration++) {

            // vertex program
            vertexIteration(iteration);


            int[] labelCounter = new int[numberOfPartitions];
            graph.getVertexStream().map(v -> ((SCLP_VertexData) v.getVertexData())).filter(Objects::nonNull).forEach(d -> labelCounter[d.label] += 1);
            logger.debug(Arrays.toString(labelCounter));

            // equalize cv every x iterations
            if (iteration % rebalancingRate == rebalancingRate - 1) {

                int maxPartition = Integer.MIN_VALUE;
                for (int i = 0; i < partitionCounter.length(); i++) {
                    maxPartition = Integer.max(maxPartition, partitionCounter.get(i));
                }

                logger.debug("REBALANCE");
                Double[] cvPools = new Double[numberOfPartitions];


                for (int i = 0; i < numberOfPartitions; i++) {
                    double baseValue = 0.1;
                    cvPools[i] = baseValue + (1 - baseValue) * ((double) maxPartition - partitionCounter.get(i)) / maxPartition;
                }

                graph.getVertexStream().parallel().forEach(vertex -> {
                    SCLP_VertexData data = ((SCLP_VertexData) vertex.getVertexData());
                    if (data != null) {
                        data.creditValue = cvPools[data.label] / this.partitionCounter.get(data.label);
                    }
                });
            }

        }

        // accumulate partitions
        // gather the clusters
        TIntObjectHashMap<Partition> partitions = accumulateClusters(graph);

        return new ArrayList<>(Arrays.asList(partitions.values(new Partition[partitions.size()])));
    }

    @Override
    public String getStrategyName() {
        return "SplitCreditLabelPropagation-cc";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName() + " (SCLP-cc) - iterations: " + iterations;
    }

}
