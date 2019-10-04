package model;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.set.hash.TCustomHashSet;

import java.util.ArrayList;
import java.util.List;

public class Vertex {

    public final long id;
    private List<Hyperedge> adjacentHyperedges;

    // vertex data for GAS algorithms
    private Object vertexData;
    private boolean active;

    // vertex data for the next iteration
    private Object nextVertexData;
    private boolean activeNextRound;

    public Vertex(long id) {
        this.id = id;
        this.adjacentHyperedges = new ArrayList<>();
        this.vertexData = null;
        this.nextVertexData = null;
        this.active = false;
        this.activeNextRound = false;
    }

    public List<Hyperedge> getAdjacentHyperedges() {
        return adjacentHyperedges;
    }

    public void addAdjacentHyperedge(Hyperedge edge) {
        this.adjacentHyperedges.add(edge);
    }

    @Override
    public int hashCode() {

        return (int) this.id;
    }

    @Override
    public boolean equals(Object v2) {

        if (!(v2.getClass().equals(this.getClass()))) {
            return false;
        }
        return this.id == ((Vertex) v2).id;
    }


    /**
     * Returns all neighbors of the vertex
     * @return set of neighbors
     */
    public TCustomHashSet<Vertex> getNeighbors() {
        TCustomHashSet<Vertex> neighborSet = new TCustomHashSet<>(new TVertexHashing());

        for (Hyperedge edge : this.adjacentHyperedges) {
            TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();

            while (it.hasNext()) {
                it.advance();
                Vertex next = it.value();
                if (next.equals(this) || neighborSet.contains(next)) {
                    continue;
                }

                neighborSet.add(next);
            }
        }

        return neighborSet;
    }

    public Object getVertexData() {
        return vertexData;
    }

    public void setVertexData(Object vertexData) {
        this.vertexData = vertexData;
    }

    public Object getNextVertexData() {
        return nextVertexData;
    }

    public void setNextVertexData(Object nextVertexData) {
        this.nextVertexData = nextVertexData;
    }

    /**
     * takes the data for the next iteration and sets it as current vertex data.
     */
    public void nextStep() {
        // check to avoid null data when the method is called on an inactive vertex
        if (this.nextVertexData != null) {
            this.vertexData = this.nextVertexData;
            this.nextVertexData = null;
        }
        this.active = activeNextRound;
        this.activeNextRound = false;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public int getDegree() {
        return this.adjacentHyperedges.size();
    }

    public boolean isActiveNextRound() {
        return activeNextRound;
    }

    public void setActiveNextRound(boolean activeNextRound) {
        this.activeNextRound = activeNextRound;
    }

    /**
     * sets the activeNextRound flag to true for all neighbors of the vertex
     */
    public void activateAllNeighborsInNextRound() {
        for (Hyperedge edge : this.getAdjacentHyperedges()) {

            // loop vertices
            TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();
            while (it.hasNext()) {
                it.advance();
                it.value().setActiveNextRound(true);
            }
        }
    }
}
