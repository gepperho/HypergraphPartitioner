package model;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TCustomHashSet;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

public class Hypergraph {

    TLongObjectHashMap<Hyperedge> hyperedges;
    TLongObjectHashMap<Vertex> vertices;


    public Hypergraph() {
        this.hyperedges = new TLongObjectHashMap<>();
        this.vertices = new TLongObjectHashMap<>();
    }

    public Vertex getVertex(long id) {
        return this.vertices.get(id);
    }

    public Hyperedge getHyperedge(long id) {
        return this.hyperedges.get(id);
    }

    /**
     *
     * @return the number of hyperedges
     */
    public int getSize() {
        return hyperedges.size();
    }

    /**
     *
     * @return the number of vertices
     */
    public int getOrder() {
        return this.vertices.size();
    }

    public TLongObjectIterator<Vertex> getVertexIterator() {
        return this.vertices.iterator();
    }

    public TLongObjectIterator<Hyperedge> getEdgeIterator() {
        return this.hyperedges.iterator();
    }

    public void addNewEdgeByFileLine(long edgeId, long[] adjacentVertices) {
        Hyperedge tempEdge = new Hyperedge(edgeId);

        for (long lVertexId: adjacentVertices) {

            Vertex tempVertex = this.vertices.get(lVertexId);
            if (tempVertex == null) {
                tempVertex = new Vertex(lVertexId);
                this.vertices.put(tempVertex.id, tempVertex);
            }

            tempVertex.addAdjacentHyperedge(tempEdge);
            tempEdge.addAdjacentVertex(tempVertex);
        }

        this.hyperedges.put(tempEdge.id, tempEdge);
    }

    /**
     * @return a random vertex from the hypergraph
     */
    public Vertex getRandomVertex() {
        Random random = new Random();
        int randomIndex = random.nextInt(vertices.size());
        return vertices.get(this.vertices.keys()[randomIndex]);
    }

    /**
     * removes a vertex from the hypergraph and its adjacent edges. If an adjacent edge would have no attached vertices anymore, the edge is also removed.
     * @param vertex vertex to be removed
     */
    public void removeVertex(Vertex vertex) {
        this.vertices.remove(vertex.id);
        for (Hyperedge edge: vertex.getAdjacentHyperedges()) {
            if (edge.getSize() == 1) {
                // remove edge since this vertex was the last adjacent vertex
                hyperedges.remove(edge.id);
            } else {
                // remove the vertex from the edge to avoid null pointers
                edge.removeAdjacentVertex(vertex);
            }
        }
    }


    public Stream<Vertex> getVertexStream() {
        return Arrays.stream(this.vertices.values(new Vertex[this.getOrder()]));
    }

    /**
     * adds a vertex to the graph. Adds also its adjacent hyperedges.
     * @param vertex vertex to be added
     */
    public void addVertex(Vertex vertex) {

        this.addVertexOnly(vertex);

        for (Hyperedge e: vertex.getAdjacentHyperedges()) {
            this.addEdgeOnly(e);
        }
    }

    /**
     * adds a vertex to the graph, without adding its adjacent edges
     * @param vertex vertex to be added
     */
    public void addVertexOnly(Vertex vertex) {
        if (this.vertices.contains(vertex.id)) {
            return;
        }

        this.vertices.put(vertex.id, vertex);
    }



    /**
     * adds a hyperedge to the graph. Adds also its adjacent vertices.
     * @param edge edge to be added
     */
    public void addEdge(Hyperedge edge) {

        this.addEdgeOnly(edge);

        TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();

        while (it.hasNext()) {
            it.advance();
            this.addVertexOnly(it.value());
        }
    }

    /**
     * adds a hyperedge to the graph, without adding its adjacent vertices.
     * @param edge hyperedge to be added
     */
    public void addEdgeOnly(Hyperedge edge) {
        if (this.hyperedges.contains(edge.id)) {
            return;
        }

        this.hyperedges.put(edge.id, edge);
    }

    public boolean containsVertex(Vertex v) {
        return this.vertices.contains(v.id);
    }

    public boolean containsVertexId(long id) {
        return this.vertices.containsKey(id);
    }

    public boolean containsEdgeId(long id) {
        return this.hyperedges.containsKey(id);
    }


    /**
     * returns all vertices neighboring the given vertex which are also stored in this graph.
     * Can be used, if a partitioning has already taken place.
     *
     * @param vertex vertex where the local neighbors are to be returned
     * @return neighbors on the same partition/graph
     */
    public TCustomHashSet<Vertex> getLocalVertexNeighbors(Vertex vertex) {
        TCustomHashSet<Vertex> neighborSet = new TCustomHashSet<>(new TVertexHashing());

        for (Hyperedge edge : vertex.getAdjacentHyperedges()) {
            TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();

            while (it.hasNext()) {
                it.advance();
                Vertex next = it.value();
                if (next.equals(vertex) || neighborSet.contains(next) || !this.containsVertexId(next.id)) {
                    continue;
                }

                neighborSet.add(next);
            }
        }

        return neighborSet;
    }

}
