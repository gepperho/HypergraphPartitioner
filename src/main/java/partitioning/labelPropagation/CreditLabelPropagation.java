package partitioning.labelPropagation;

import evaluation.TimeMeasurement;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import model.Hyperedge;
import model.Hypergraph;
import model.Partition;
import model.Vertex;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import partitioning.GreedyWeightedVertex;
import partitioning.PartitioningStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CreditLabelPropagation implements PartitioningStrategy {

    private final static Logger logger = LogManager.getLogger(CreditLabelPropagation.class);

    // magic numbers for the desired seed degree
    static final int seedMinDegree = 1000;
    static final int seedMaxDegree = 10000;

    @Override
    public List<Partition> partition(Hypergraph graph, int numberOfPartitions) {

        this.graph = graph;
        // initialize seeds
        for (int i = 0; i < numberOfPartitions * m; i++) {
            initializeNewSeed(i);
        }

        // run lp iterations
        for (int iteration = 0; iteration < iterations; iteration++) {
            TimeMeasurement iterationTime = new TimeMeasurement();
            iterationTime.startMeasurement();
            graph.getVertexStream().parallel().filter(Vertex::isActive).forEach(this::propagateLabelAndCreditValue);

            iterationTime.endMeasurement();

            if (logger.getLevel().compareTo(Level.DEBUG) >= 0) {
                logger.debug(iteration + ": " + iterationTime.getTimeDiferenceInMillis());
                // count vertices with data
                logger.debug("Active next round: " + graph.getVertexStream().parallel().filter(Vertex::isActiveNextRound).count());
            }
            // apply update
            graph.getVertexStream().parallel().forEach(Vertex::nextStep);
        }

        if (logger.getLevel().compareTo(Level.DEBUG) >= 0) {

            // verification code to check how the labels are distributed.
            int[] labelArray = new int[numberOfPartitions * m];
            graph.getVertexStream().filter(v -> v.getVertexData() != null).forEach(v -> {
                int label = ((CLP_VertexData) v.getVertexData()).Label;
                if (label < 0 || label >= numberOfPartitions * m) {
                    System.err.println("invalid label: " + label);
                }
                labelArray[label]++;
            });
            // TODO handle vertices with label -1 (no label assigned)
            int[] x = Arrays.stream(labelArray).filter(e -> e != 0).boxed().sorted(Collections.reverseOrder()).mapToInt(e -> e).toArray();
            logger.debug(Arrays.toString(x));
            logger.debug(x.length + "/" + (numberOfPartitions * m));
        }

        // gather the clusters
        TIntObjectHashMap<Partition> partitions = new TIntObjectHashMap<>();
        graph.getVertexStream().forEach(vertex -> {
            int label = ((CLP_VertexData) vertex.getVertexData()).Label;
            if (partitions.containsKey(label)) {
                partitions.get(label).addVertexToPartition(vertex);
            } else {
                partitions.put(label, new Partition(label));
            }
        });


        Hypergraph accumulatedGraph = new Hypergraph();
        TIntObjectIterator<Partition> it = partitions.iterator();
        while (it.hasNext()) {
            it.advance();
            accumulatedGraph.addVertex(partitionToVertex(it.value()));
        }

        PartitioningStrategy greedy = new GreedyWeightedVertex();

        List<Partition> clusterPartitioning = greedy.partition(accumulatedGraph, numberOfPartitions);

        // map the labels to their partitions
        TIntIntHashMap labelMapping = new TIntIntHashMap();

        for (Partition p: clusterPartitioning) {
            TLongObjectIterator<Vertex> vertexIt = p.getVertexIterator();

            while (vertexIt.hasNext()) {
                vertexIt.advance();
                Vertex vertex = vertexIt.value();
                int label = (int) vertex.id;
                labelMapping.put(label, p.getId());
            }
        }

        // recreate the final partitioning
        List<Partition> finalPartitioning = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            finalPartitioning.add(new Partition(i));
        }

        graph.getVertexStream().forEach(vertex -> {
            int label = ((CLP_VertexData) vertex.getVertexData()).Label;
            finalPartitioning.get(labelMapping.get(label)).addVertexToPartition(vertex);
        });


        return finalPartitioning;
    }

    private Vertex partitionToVertex(Partition p) {
        Vertex newVertex = new Vertex(p.getId());
        newVertex.setVertexData((long) p.getOrder());

        TLongObjectIterator<Hyperedge> it = p.getEdgeIterator();
        while (it.hasNext()) {
            it.advance();
            newVertex.addAdjacentHyperedge(it.value());
        }

        return newVertex;
    }

    @Override
    public String getStrategyName() {
        return "CreditLabelPropagation";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName() + " (CLP) - iterations: " + iterations + " - seed multiplier (m): " + m;
    }

    private Hypergraph graph;

    // multiplier for the number of seeds (m * n)
    private int m;
    private int iterations;

    public CreditLabelPropagation(int iterations, int m) {
        this.m = m;
        this.iterations = iterations;

    }

    private void initializeNewSeed(int label) {

        // find a vertex without a seed
        Vertex tempVertex;
        do {
            tempVertex = this.graph.getRandomVertex();
        } while (tempVertex.getVertexData() != null);

        // move seed
        final int maxSteps = 3;
        for (int i = 0; i < maxSteps; i++) {
            tempVertex = moveSeed(tempVertex);
        }

        tempVertex.setVertexData(new CLP_VertexData(label, 1));
        tempVertex.setActive(true);
    }

    /**
     * generic method wo walk from the given vertex to a vertex closer to the desired degree range
     * @param seedVertex given vertex
     * @return the next vertex (same vertex if the seedVertex is already in the degree range)
     */
    public static Vertex moveSeed(Vertex seedVertex) {

        if (seedVertex.getDegree() >= seedMinDegree && seedVertex.getDegree() <= seedMaxDegree) {
            // no move needed
            return seedVertex;
        }

        Vertex fittingNeighbor = null;
        float fittingDegree = Float.MAX_VALUE;
        for (Hyperedge edge : seedVertex.getAdjacentHyperedges()) {
            TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();
            while (it.hasNext()) {
                it.advance();
                Vertex neighbor = it.value();

                if (neighbor.getVertexData() != null) {
                    // vertex has already a label
                    // TODO fix to allow walking over neighbors with a label
                    continue;
                }

                if (fittingNeighbor == null) {
                    // initialize neighbor
                    fittingNeighbor = neighbor;
                    continue;
                }

                if (fittingNeighbor.equals(neighbor)) {
                    // prevent calculation of same vertex if connected via multiple edges
                    continue;
                }

                // compare fitting to move
                float neighborFitting;
                int neighborDegree = neighbor.getDegree();
                if (neighborDegree < seedMinDegree) {
                    neighborFitting = seedMinDegree - neighborDegree;
                } else if (neighborDegree > seedMaxDegree) {
                    neighborFitting = neighborDegree - seedMaxDegree;
                } else {
                    neighborFitting = ((float) neighborDegree - seedMinDegree) / (seedMaxDegree - seedMinDegree);
                }

                if (neighborFitting < fittingDegree) {
                    fittingDegree = neighborFitting;
                    fittingNeighbor = neighbor;
                }
            }
        }

        // best option
        if (fittingNeighbor == null) {
            return seedVertex;
        }
        return fittingNeighbor;
    }

    /**
     * the vertex program to update a vertex label and cv
     *
     * @param vertex the vertex to be updated
     */
    private void propagateLabelAndCreditValue(Vertex vertex) {
        TIntDoubleHashMap labelMap = new TIntDoubleHashMap();
        // counter to get the number of input values for later normalisation
        int elementCounter = 0;

        // gather
        for (Hyperedge edge : vertex.getAdjacentHyperedges()) {
            double edgeFraction = calculateEdgeReduction(edge);

            // loop vertices
            TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();
            while (it.hasNext()) {
                it.advance();
                if (it.key() == vertex.id) {
                    // ignore the own vertex
                    continue;
                }
                Vertex next = it.value();

                next.setActiveNextRound(true);

                CLP_VertexData neighborData = (CLP_VertexData) next.getVertexData();
                if (neighborData == null) {
                    // neighbor has no data yet
                    continue;
                }
                elementCounter++;

                labelMap.put(neighborData.Label, labelMap.get(neighborData.Label) + edgeFraction * neighborData.creditValue);

            }
        }

        // apply
        // find max normalized value
        int maxKey = -1;
        double maxValue = Double.MIN_VALUE;
        for (int key: labelMap.keys()) {
            double normalizedCreditValue = labelMap.get(key)/elementCounter;

            if (normalizedCreditValue > maxValue) {
                maxValue = normalizedCreditValue;
                maxKey = key;
            }
        }

        // if no proper next value is found, do nothing (can happen, if no neighbor has a label yet)
        if (maxKey != -1) {
            vertex.setNextVertexData(new CLP_VertexData(maxKey, maxValue));
        }
    }

    /**
     * calculates the reduction factor of an edge for the passed cv
     *
     * @param edge edge parameter for the formula
     * @return reduction factor of the edge
     */
    private double calculateEdgeReduction(Hyperedge edge) {
        // magic values of the reduction function
        final double a = 0.2;
        final double b = 60;
        double x = a * Math.pow(edge.getSize() - 2, 2);
        return 1.0 - x / (x + b);
    }

    /**
     * class holding the CLP vertex data (label, credit value)
     */
    private static class CLP_VertexData {

        int Label;
        double creditValue;

        public CLP_VertexData(int label, double creditValue) {
            Label = label;
            this.creditValue = creditValue;
        }
    }
}
