package model;

import gnu.trove.iterator.TLongObjectIterator;

import java.util.ArrayList;
import java.util.List;

public class Partition extends  Hypergraph{

    private final int id;

    public Partition(int id) {
        super();
        this.id = id;
    }


    public void addVertexToPartition(Vertex v) {
        this.vertices.put(v.id, v);

        for (Hyperedge edge: v.getAdjacentHyperedges()) {
            if (!this.hyperedges.contains(edge.id)) {
                this.hyperedges.put(edge.id, edge);
            }
        }
    }


    public int getId() {
        return id;
    }

    /**
     * @return hyperedges, which are not completely on this partition (at least one vertex is missing)
     */
    public List<Hyperedge> getIncompleteEdges() {

        List<Hyperedge> incompleteEdges = new ArrayList<>();

        // loop edges adjacent to this partition
        TLongObjectIterator<Hyperedge> it = this.hyperedges.iterator();
        edgeLoop: while (it.hasNext()) {
            it.advance();
            Hyperedge edge = it.value();

            TLongObjectIterator<Vertex> vertexIterator = edge.getAdjacentVerticesIterator();

            // loop vertices adjacent to the currently looped edge
            while(vertexIterator.hasNext()) {
                vertexIterator.advance();
                Vertex vertex = vertexIterator.value();

                // check if each vertex is part of this partition
                if (!this.vertices.contains(vertex.id)) {

                     incompleteEdges.add(edge);
                     continue edgeLoop;
                }
            }
        }

        return incompleteEdges;
    }
}
