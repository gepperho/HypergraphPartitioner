package partitioning;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import model.Hyperedge;
import model.Hypergraph;
import model.Partition;
import model.Vertex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Partitions a graph with weighted vertices. The weight is stored as long value in the vertexData.
 * Strategy is used within CLP
 */
public class GreedyWeightedVertex implements PartitioningStrategy {
    @Override
    public List<Partition> partition(Hypergraph graph, int numberOfPartitions) {

        // initialisation
        ArrayList<Partition> partitioning = new ArrayList<>(numberOfPartitions);
        for (int i = 0; i < numberOfPartitions; i++) {
            partitioning.add(new Partition(i));
        }

        long weightedSize = graph.getVertexStream().mapToLong(v -> (long) v.getVertexData()).sum();
        long balancedWeightedSize = weightedSize % numberOfPartitions == 0 ? weightedSize / numberOfPartitions : (weightedSize / numberOfPartitions) + 1;

        final long[] weightedPartitionSizes = new long[numberOfPartitions];
        final int[] counter = {0};
        graph.getVertexStream()
                .sorted(Comparator.comparingLong(v -> (long) v.getVertexData() * -1))
                .forEachOrdered(vertex -> {


                    // hash the first 4 values
                    if (counter[0] < numberOfPartitions) {
                        partitioning.get(counter[0]).addVertexToPartition(vertex);
                        weightedPartitionSizes[counter[0]] += (long) vertex.getVertexData();
                        counter[0]++;
                        return;
                    }
                    counter[0]++;
                    long vertexWeight = (long) vertex.getVertexData();


                    TIntDoubleHashMap fittings = new TIntDoubleHashMap();

                    for (Partition p : partitioning) {
                        fittings.put(p.getId(), calculateIntersection(vertex, p));
                    }

                    boolean done = false;
                    do {

                        int max;

                        // special case: balance is always mismatched
                        if (fittings.isEmpty()) {
                            max = getIndexOfMinValue(weightedPartitionSizes);

                        } else {

                            max = retrieveMaxValue(fittings);

                            // check balancing constraint
                            if (!(weightedPartitionSizes[max] + vertexWeight <= balancedWeightedSize)) {
                                fittings.remove(max);
                                continue;

                            }
                        }

                        partitioning.get(max).addVertexToPartition(vertex);
                        weightedPartitionSizes[max] += vertexWeight;
                        done = true;

                    } while (!done);
                });

        return partitioning;
    }

    @Override
    public String getStrategyName() {
        return "GreedyAssignmentOfWeightedVertices";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName();
    }

    private int calculateIntersection(Vertex v, Partition p) {
        int intersection = 0;

        //  TODO fix. Vertices sometimes have for some reason no adjacent edges...

        // todo improve intersection metric
        for (Hyperedge h : v.getAdjacentHyperedges()) {
            if (p.containsEdgeId(h.id)) {
                intersection++;
            }
        }

        return intersection;
    }

    private int retrieveMaxValue(TIntDoubleHashMap map) {
        TIntDoubleIterator it = map.iterator();

        double maxValue = -1.0;
        int maxKey = 0;

        while (it.hasNext()) {
            it.advance();
            double elementValue = it.value();
            int elementKey = it.key();

            if (elementValue > maxValue) {
                maxValue = elementValue;
                maxKey = elementKey;
            }

        }

        return maxKey;
    }

    private int getIndexOfMinValue(long[] array) {
        long minValue = Long.MAX_VALUE;
        int minId = 0;

        for (int i = 0; i < array.length; i++) {
            if (array[i] < minValue) {
                minValue = array[i];
                minId = i;
            }
        }

        return minId;
    }
}
