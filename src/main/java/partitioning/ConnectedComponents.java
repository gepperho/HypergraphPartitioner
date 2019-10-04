package partitioning;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongIntHashMap;
import model.Hyperedge;
import model.Hypergraph;
import model.Partition;
import model.Vertex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * partitioning strategy which maps each connected component to a separate partition.
 * Can be used to retrieve the largest connected component of a data set.
 */
public class ConnectedComponents implements PartitioningStrategy{

    private final boolean printOnlyLargestComponent;

    public ConnectedComponents(boolean printOnlyLargestComponent) {
        this.printOnlyLargestComponent = printOnlyLargestComponent;
    }

    @Override
    public List<Partition> partition(Hypergraph graph, int numberOfPartitions) {


        // initialize seeds
        graph.getVertexStream().forEach(v -> {
            v.setActive(true);
            v.setVertexData(v.id);
        });

        // run iterations
        while (graph.getVertexStream().anyMatch(Vertex::isActive)) {
            graph.getVertexStream().parallel().filter(Vertex::isActive).forEach(this::vertexProgram);

            // apply update
            graph.getVertexStream().parallel().forEach(Vertex::nextStep);
        }


        // map the vertices to their label
        int partitionCounter = 0;
        TLongIntHashMap labelToPositionMapping = new TLongIntHashMap();
        ArrayList<Partition> partitioning = new ArrayList<>();

        TLongObjectIterator<Vertex> it = graph.getVertexIterator();

        while (it.hasNext()) {

            it.advance();
            Vertex vertex = it.value();

            long label = (long) vertex.getVertexData();
            if (labelToPositionMapping.containsKey(label)) {
                partitioning.get(labelToPositionMapping.get(label)).addVertexToPartition(vertex);
            } else {
                labelToPositionMapping.put(label, partitionCounter);
                Partition tempPartition = new Partition(partitionCounter);
                partitioning.add(tempPartition);
                partitionCounter++;

                tempPartition.addVertexToPartition(vertex);
            }
        }

        if (printOnlyLargestComponent) {
            // return only the largest partition (to avoid storing too much unnecessary files on the HDD)
            partitioning.sort( Comparator.comparingInt(p -> p.getOrder() * -1));
            Partition largest = partitioning.get(0);
            partitioning.clear();
            partitioning.add(largest);
        }

        return partitioning;
    }


    /**
     * the vertex program to update a vertex label.
     * The vertex takes the smallest label of all neighbors labels
     *
     * @param vertex the vertex to be updated
     */
    private void vertexProgram(Vertex vertex) {

        long ownLabel = (long) vertex.getVertexData();
        long minId = ownLabel;

        // find minimal label in the neighborhood
        for (Hyperedge edge : vertex.getAdjacentHyperedges()) {

            // loop vertices
            TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();
            while (it.hasNext()) {
                it.advance();
                long nextLabel = (long) it.value().getVertexData();

                if (nextLabel < minId) {
                    minId = nextLabel;
                }

            }
        }

        if (minId == ownLabel) {
            // no label change
            return;
        }

        // update
        vertex.setNextVertexData(minId);
        // activate all neighbors
        vertex.activateAllNeighborsInNextRound();
    }

    @Override
    public String getStrategyName() {
        return "ConnectedComponents";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName() + " (CC)";
    }
}
