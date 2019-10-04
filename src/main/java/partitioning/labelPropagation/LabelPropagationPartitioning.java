package partitioning.labelPropagation;

import evaluation.TimeMeasurement;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import model.Hyperedge;
import model.Hypergraph;
import model.Partition;
import model.Vertex;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import partitioning.PartitioningStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class LabelPropagationPartitioning implements PartitioningStrategy {

    private final static Logger logger = LogManager.getLogger(LabelPropagationPartitioning.class);

    private int iterations;
    protected Hypergraph graph;
    protected int numberOfPartitions;

    private AtomicIntegerArray partitionCounter;


    public LabelPropagationPartitioning(int iterations) {
        this.iterations = iterations;
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
        initializeSeeds();


        for (int iteration = 0; iteration < iterations; iteration++) {

            TimeMeasurement timer = new TimeMeasurement();
            timer.startMeasurement();


            logger.trace("selectLabel...");
            // iterate vertices - select label
            graph.getVertexStream().parallel().forEach(this::selectLabel);

            partitionCounter = new AtomicIntegerArray(numberOfPartitions); // clear array


            logger.trace("accumulateCreditValue...");
            // iterate vertices - apply received
            graph.getVertexStream().parallel().forEach(v -> {
                int nextLabel = (int) v.getNextVertexData();

                // update partition counter
                partitionCounter.incrementAndGet(nextLabel);
                v.nextStep();
            });

            int maxPartition = Integer.MIN_VALUE;
            for (int i = 0; i < partitionCounter.length(); i++) {
                maxPartition = Integer.max(maxPartition, partitionCounter.get(i));
            }


            // debug output
            timer.endMeasurement();
            logger.debug("Iteration " + (iteration + 1) + " - " + timer.getTimeDiferenceInMillis() + " ms");


            if (logger.getLevel().compareTo(Level.DEBUG) >= 0) {
                AtomicIntegerArray labelCounter = new AtomicIntegerArray(numberOfPartitions);
                graph.getVertexStream().parallel()
                        .map(v -> ((int) v.getVertexData()))
                        .forEach(labelCounter::getAndIncrement);
                logger.debug(labelCounter.toString());
            }

        }

        // accumulate partitions
        // gather the clusters
        TIntObjectHashMap<Partition> partitions = new TIntObjectHashMap<>();
        graph.getVertexStream().forEach(vertex -> {
            int label = (int) vertex.getVertexData();
            if (partitions.containsKey(label)) {
                partitions.get(label).addVertexToPartition(vertex);
            } else {
                partitions.put(label, new Partition(label));
                partitions.get(label).addVertexToPartition(vertex);
            }
        });

        return new ArrayList<>(Arrays.asList(partitions.values(new Partition[partitions.size()])));
    }

    @Override
    public String getStrategyName() {
        return "LabelPropagationPartitioning";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName() + " (LPP) - iterations: " + iterations;
    }


    /**
     * random seed initialization
     */
    private void initializeSeeds() {
        graph.getVertexStream().parallel().forEach(vertex -> {
            int label = (int) (Math.random() * numberOfPartitions);
            vertex.setVertexData(label);
            partitionCounter.incrementAndGet(label);
        });
    }

    /**
     * set the label the vertex will have in the next iteration
     *
     * @param vertex vertex to be calculated
     */
    private void selectLabel(Vertex vertex) {

        int[] labelMap = new int[numberOfPartitions];

        // fill the labelMap using a pessimistic heuristic
        for (Hyperedge e : vertex.getAdjacentHyperedges()) {

            // get label appearing the most in this edge
            int[] edgeLabelMap = new int[numberOfPartitions];


            TLongObjectIterator<Vertex> it = e.getAdjacentVerticesIterator();

            while (it.hasNext()) {
                it.advance();
                if (it.key() == vertex.id) {
                    continue;
                }
                Vertex neighbor = it.value();

                int neighborLabel = (int) neighbor.getVertexData();

                edgeLabelMap[neighborLabel] += 1;
            }

            int maxAppearingInEdge = getIndexOfMaxElement(edgeLabelMap);
            if (maxAppearingInEdge >= 0) {
                labelMap[maxAppearingInEdge] += 1;
            }

        }


        // find the arg max
        int maxKey = 0;
        double maxValue = 0;
        for (int key = 0; key < labelMap.length; key++) {
            double value = labelMap[key];
            double balancingPunishment = calculateLppBalancingFactor(key, this.partitionCounter, 2);
            if (value * balancingPunishment > maxValue) {
                maxValue = value * balancingPunishment;
                maxKey = key;
            }
        }

        vertex.setNextVertexData(maxKey);
    }

    /**
     *
     * @param array input array
     * @return the key of the largest element of 'array'
     */
    private int getIndexOfMaxElement(int[] array) {
        // find the arg max
        int maxKey = -1;
        int maxValue = 0;
        for (int key = 0; key < array.length; key++) {
            int value = array[key];

            if (value > maxValue) {
                maxValue = value;
                maxKey = key;
            }
        }

        return maxKey;
    }


    /**
     * Balancing factor calculation of LPP, as explained in the hyperX paper.
     * @param label labelId
     * @param partitionCounter cluster sizes
     * @param exponent exponent of the cluster sizes ((in the paper 2 was used)
     * @return balancing factor
     */
    public static double calculateLppBalancingFactor(int label, AtomicIntegerArray partitionCounter, int exponent) {
        int labelSize = partitionCounter.get(label);

        int sum = 0;
        for (int i = 0; i < partitionCounter.length(); i++) {
            sum += partitionCounter.get(i);
        }
        double average = ((double) sum) / partitionCounter.length();

        double squareAverage = Math.pow(average, exponent);

        double e_exponent = (squareAverage - Math.pow(labelSize, exponent)) / squareAverage;

        return Math.pow(Math.E, e_exponent);
    }
}
