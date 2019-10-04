package partitioning;

import model.Hyperedge;
import model.Hypergraph;
import model.Partition;
import model.Vertex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class MinMaxNB implements PartitioningStrategy {


    private final int allowedImbalanceInVertices = 100;

    @Override
    public List<Partition> partition(Hypergraph graph, int numberOfPartitions) {

        ArrayList<Partition> partitioning = new ArrayList<>(numberOfPartitions);

        for (int i = 0; i < numberOfPartitions; i++) {
            partitioning.add(new Partition(i));
        }


        graph.getVertexStream().sequential().forEach(vertex -> {

            int[] commonEdges = new int[numberOfPartitions];

            int minSize = Integer.MAX_VALUE;

            // calculate fitting & find max partition size
            for (Partition p : partitioning) {
                commonEdges[p.getId()] = calculateCommonEdges(vertex, p);
                if (p.getOrder() < minSize) {
                    minSize = p.getOrder();
                }
            }

            // check fittings
            for (int index: orderFitting(commonEdges)) {

                // check balancing constraint
                if (partitioning.get(index).getOrder() + 1 <= minSize + allowedImbalanceInVertices ) {
                    partitioning.get(index).addVertexToPartition(vertex);
                    return;
                }
            }
        });


        return partitioning;
    }

    private int calculateCommonEdges(Vertex vertex, Partition p) {

        int counter = 0;

        for (Hyperedge edge : vertex.getAdjacentHyperedges()) {
            if (p.containsEdgeId(edge.id)) {
                counter++;
            }
        }

        return counter;
    }

    private int[] orderFitting(int[] list) {


        return IntStream.range(0, list.length)
                .boxed()
                .sorted(Comparator.comparingInt(i -> list[i] * -1))
                .mapToInt(ele -> ele).toArray();
    }

    @Override
    public String getStrategyName() {
        return "MinMaxNB";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName();
    }
}
