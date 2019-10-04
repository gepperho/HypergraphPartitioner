package model;

import gnu.trove.strategy.HashingStrategy;

/**
 * Hashing Strategy for TCustomHashMaps to store vertices from the Vertex class
 */
public class TVertexHashing implements HashingStrategy<Vertex> {
    @Override
    public int computeHashCode(Vertex vertex) {
        return vertex.hashCode();
    }

    @Override
    public boolean equals(Vertex vertex, Vertex t1) {
        return vertex.equals(t1);
    }
}
