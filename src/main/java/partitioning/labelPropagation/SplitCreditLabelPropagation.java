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

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class SplitCreditLabelPropagation implements PartitioningStrategy {

    private final static Logger logger = LogManager.getLogger(SplitCreditLabelPropagation.class);

    protected int iterations;
    protected Hypergraph graph;
    protected int numberOfPartitions;

    private TIntObjectHashMap<SCLP_VertexData> masterVertices;
    protected AtomicIntegerArray partitionCounter;

    protected int balancing_e_exponent = 2;

    public SplitCreditLabelPropagation(int iterations) {
        this.iterations = iterations;
        this.masterVertices = new TIntObjectHashMap<>();
    }

    @Override
    public List<Partition> partition(Hypergraph graph, int numberOfPartitions) {

        this.graph = graph;
        partitionCounter = new AtomicIntegerArray(numberOfPartitions);

        // initialize with 1
        for (int i = 0; i < partitionCounter.length(); i++) {
            partitionCounter.set(i, 1);
        }


        this.numberOfPartitions = numberOfPartitions;

        // initialize seeds
        initializeSeeds(numberOfPartitions);

        for (int iteration = 0; iteration < iterations; iteration++) {

            // vertex program
            vertexIteration(iteration);

            if (logger.getLevel().compareTo(Level.DEBUG) >= 0) {
                AtomicIntegerArray labelCounter = new AtomicIntegerArray(numberOfPartitions);
                graph.getVertexStream().parallel().map(v -> ((SCLP_VertexData) v.getVertexData())).filter(Objects::nonNull).forEach(d -> labelCounter.getAndIncrement(d.label));
                logger.debug(labelCounter.toString());
            }
        }

        // accumulate partitions
        TIntObjectHashMap<Partition> partitions = accumulateClusters(graph);

        return new ArrayList<>(Arrays.asList(partitions.values(new Partition[partitions.size()])));
    }

    @Override
    public String getStrategyName() {
        return "SplitCreditLabelPropagation";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName() + " (SCLP) - iterations: " + iterations;
    }


    /**
     * calls the selectLabel, splitCreditValue and applyCreditValue method on the whole graph.
     */
    protected void vertexIteration(int iteration) {

        TimeMeasurement timer = new TimeMeasurement();
        timer.startMeasurement();

        // iterate vertices - select label
        graph.getVertexStream().parallel().forEach(this::selectLabel);


        // iterate vertices - split credit value
        graph.getVertexStream().parallel().filter(Vertex::isActive).forEach(this::splitCreditValue);


        // iterate vertices - apply received
        partitionCounter = new AtomicIntegerArray(numberOfPartitions);
        graph.getVertexStream().parallel().forEach(this::applyCreditValue);

        timer.endMeasurement();
        logger.debug("Iteration " + (iteration + 1) + " - " + timer.getTimeDiferenceInMillis() + " ms");

    }

    protected void initializeSeeds(int numberOfSeeds) {

        for (int label = 0; label < numberOfSeeds; label++) {

            Vertex tempVertex = null;
            final int maxTries = 3;

            for (int j = 0; j < maxTries; j++) {
                // find a vertex without a seed
                do {
                    tempVertex = this.graph.getRandomVertex();
                } while (tempVertex.getVertexData() != null);

                // move seed
                final int maxSteps = 3;
                for (int i = 0; i < maxSteps; i++) {
                    tempVertex = CreditLabelPropagation.moveSeed(tempVertex);
                }

                if (tempVertex.getDegree() < CreditLabelPropagation.seedMinDegree || tempVertex.getDegree() > CreditLabelPropagation.seedMaxDegree) {
                    break;
                }
            }

            SCLP_VertexData tempData = new SCLP_VertexData(label, 1);
            tempVertex.setVertexData(tempData);
            logger.debug("seed " + label + " degree: " + tempVertex.getDegree());
            checkAndUpdateMaster(tempData);
        }
    }


    /**
     * set the label the vertex will have in the next iteration
     *
     * @param vertex vertex to be calculated
     */
    private void selectLabel(Vertex vertex) {

        double[] labelMap = new double[masterVertices.size()];

        // fill the labelMap using a pessimistic heuristic
        for (Hyperedge e : vertex.getAdjacentHyperedges()) {
            TLongObjectIterator<Vertex> it = e.getAdjacentVerticesIterator();

            while (it.hasNext()) {
                it.advance();
                if (it.key() == vertex.id) {
                    continue;
                }
                Vertex neighbor = it.value();

                SCLP_VertexData neighborData = (SCLP_VertexData) neighbor.getVertexData();

                if (neighborData == null) {
                    // has no data yet
                    continue;
                }

                double balancingPunishment = LabelPropagationPartitioning.calculateLppBalancingFactor(neighborData.label, this.partitionCounter, this.balancing_e_exponent);

                labelMap[neighborData.label] += balancingPunishment * neighborData.creditValue / (2 * neighbor.getDegree() * e.getSize());
            }
        }


        // find the arg max
        int maxKey = -1;
        double maxValue = 0;
        for (int key = 0; key < labelMap.length; key++) {
            double value = labelMap[key];

            if (value > maxValue) {
                maxValue = value;
                maxKey = key;
            }
        }


        // create next iterations vertex data
        SCLP_VertexData data = ((SCLP_VertexData) vertex.getVertexData());

        if (maxKey == -1) {
            // no neighbor has a label

            if (data == null) {
                // vertex has no label yet
                vertex.setActive(false);
                return;
            }

            // vertex has a label and keeps it
            vertex.setNextVertexData(data);
        } else {
            if (data != null && data.label == maxKey) {
                // has already a cv and keeps the label
                // keep the old cv
                vertex.setNextVertexData(data);
            } else if (data != null) { // && data.label != maxKey // no need to write down, since always true if data != null
                // has already a cv but changes the label
                if (maxValue > data.creditValue) {
                    vertex.setNextVertexData(new SCLP_VertexData(maxKey, 0));
                } else {
                    vertex.setNextVertexData(data);
                }
            } else {
                vertex.setNextVertexData(new SCLP_VertexData(maxKey, 0));
            }
        }

        vertex.setActive(true);
        checkAndUpdateMaster((SCLP_VertexData) vertex.getNextVertexData());

    }

    /**
     * splits the vertex's cv and sends a part of it to its label neighbors
     *
     * @param vertex vertex to be handled
     */
    private void splitCreditValue(Vertex vertex) {

        SCLP_VertexData currentData = ((SCLP_VertexData) vertex.getVertexData());
        SCLP_VertexData nextData = ((SCLP_VertexData) vertex.getNextVertexData());
        int currentLabel;
        int nextLabel = nextData.label;

        if (currentData == null) {
            // first iteration, no label set so far
            currentLabel = nextLabel;
        } else {
            currentLabel = currentData.label;
        }


        double cvPool;
        int inspectedLabel;

        if (currentLabel == nextLabel) {
            // no change in label, send away half of the cv
            double balancingPunishment = LabelPropagationPartitioning.calculateLppBalancingFactor(nextLabel, this.partitionCounter, balancing_e_exponent);

            cvPool = (nextData.creditValue / 2) * balancingPunishment;
            nextData.creditValue -= cvPool;
            inspectedLabel = nextLabel;
        } else {
            // label has changed, the old cv needs to be send to vertices with the old label
            cvPool = currentData.creditValue;
            inspectedLabel = currentLabel;
        }

        // search for neighbors with same label
        LinkedList<LinkedList<SCLP_VertexData>> neighborsEdges = new LinkedList<>();

        // search for neighbors with the same inspectedLabel
        for (Hyperedge edge : vertex.getAdjacentHyperedges()) {

            LinkedList<SCLP_VertexData> labelNeighbors = new LinkedList<>();

            // find labelNeighbors
            TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();
            while (it.hasNext()) {
                it.advance();
                if (it.key() == vertex.id) {
                    // filter 'vertex'
                    continue;
                }

                Vertex neighbor = it.value();
                SCLP_VertexData neighborData = ((SCLP_VertexData) neighbor.getNextVertexData());
                if (neighborData != null && neighborData.label == inspectedLabel) {
                    labelNeighbors.add(neighborData);
                }

            }

            if (labelNeighbors.size() > 0) {
                neighborsEdges.add(labelNeighbors);
            }

        }

        double edgeCvPool = cvPool / neighborsEdges.size();

        for (LinkedList<SCLP_VertexData> labelNeighbors : neighborsEdges) {
            double sendCV = edgeCvPool / labelNeighbors.size();

            for (SCLP_VertexData labelNeighborData : labelNeighbors) {
                labelNeighborData.receiving.add(sendCV);
                // reduce remaining cv
                cvPool -= sendCV;
            }
        }

        // send remaining cv to master / keep it
        if (currentLabel == nextLabel) {
            // keep remaining cv
            nextData.creditValue += cvPool;
        } else {
            // remaining old label cv is send to the label master
            SCLP_VertexData master = this.masterVertices.get(inspectedLabel);
            master.receiving.add(cvPool);
        }
    }

    /**
     * applies the cv in he receiving queue and moves to the next step
     * @param vertex vertex to be handled
     */
    private void applyCreditValue(Vertex vertex) {
        SCLP_VertexData vertexData = ((SCLP_VertexData) vertex.getNextVertexData());

        // skip null values
        if (vertexData != null) {
            double received = vertexData
                    .receiving.stream()
                    .filter(Objects::nonNull)
                    .mapToDouble(Double::doubleValue)
                    .sum();
            vertexData.creditValue += received;
            vertexData.receiving.clear();

            // update partition counter
            partitionCounter.incrementAndGet(vertexData.label);
        }
        vertex.nextStep();
    }



    /**
     * checks, if the vertices data is better than the current master
     *
     * @param data current vertex's data
     */
    private synchronized void checkAndUpdateMaster(SCLP_VertexData data) {
        if (masterVertices.containsKey(data.label)) {
            // check if better
            if (!(masterVertices.get(data.label).creditValue < data.creditValue)) {
                return;
            }
        }
        masterVertices.put(data.label, data);
    }


    /**
     * takes a graph and creates partitions based on the vertices labels
     *
     * @param graph input hypergraph
     * @return partitioning map
     */
    protected TIntObjectHashMap<Partition> accumulateClusters(Hypergraph graph) {
        TIntObjectHashMap<Partition> partitions = new TIntObjectHashMap<>();
        graph.getVertexStream().forEach(vertex -> {
            int label = ((SCLP_VertexData) vertex.getVertexData()).label;
            if (partitions.containsKey(label)) {
                partitions.get(label).addVertexToPartition(vertex);
            } else {
                partitions.put(label, new Partition(label));
                partitions.get(label).addVertexToPartition(vertex);
            }
        });

        return partitions;
    }


    /**
     * class holding the SCLP vertex data (label, credit value, receiving queue)
     */
    protected class SCLP_VertexData {

        int label;
        double creditValue;

        Queue<Double> receiving;

        public SCLP_VertexData(int label, double creditValue) {
            this.label = label;
            this.creditValue = creditValue;
            this.receiving = new ConcurrentLinkedQueue<>();
        }
    }
}
