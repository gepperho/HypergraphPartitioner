package model;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

public class Hyperedge {

    public final long id;
    private TLongObjectHashMap<Vertex> adjacentVertices;

    public Hyperedge(long id) {
        this.id = id;
        this.adjacentVertices = new TLongObjectHashMap<>();
    }

    public TLongObjectIterator<Vertex> getAdjacentVerticesIterator() {
        return this.adjacentVertices.iterator();
    }

    public void addAdjacentVertex(Vertex vertex) {
        this.adjacentVertices.put(vertex.id, vertex);
    }

    /**
     * @return the number of adjacent vertices (edge cardinality)
     */
    public int getSize() {
        return this.adjacentVertices.size();
    }

    @Override
    public int hashCode() {
        return (int) this.id;
    }

    public void removeAdjacentVertex(Vertex vertex) {
        this.adjacentVertices.remove(vertex.id);
    }

}
