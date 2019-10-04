package io;

import model.Hyperedge;
import model.Hypergraph;
import model.Vertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class GraphReader {

    public enum InputFormat {
        EDGE_LIST, BIPARTITE
    }

    private final static Logger logger = LogManager.getLogger(GraphReader.class);

    /**
     * Takes a given file (path) and an input format and reads the file according to the format.
     * Can change the format according to the file format.
     *
     * @param filePath path of the input file
     * @param format format of the input file
     * @return a hypergraph object
     * @throws IOException throws an exception, if the file extension and format don't fit together and the issue could
     * not be fixed automatically or if the file was not found
     */
    public static Hypergraph readHypergraph(Path filePath, InputFormat format) throws IOException {

        if (format == InputFormat.EDGE_LIST) {
            return readHyperedgeListFile(filePath);
        } else {
            if (filePath.toString().substring(filePath.toString().lastIndexOf(".")).equals(".hList")) {
                logger.info("File extension of input file matches hList. Using EDGE_LIST input reader instead.");
                return readHyperedgeListFile(filePath);
            }

            return readBipartiteGraph(filePath);
        }
    }



    /**
     * Takes a given file (path) of a hList file and reads the file as a list of hyperedges
     * @param filePath path of the hyperedge list
     * @return a graph object holding the full graph
     */
    private static Hypergraph readHyperedgeListFile(Path filePath) throws IOException {

        if (!filePath.toString().substring(filePath.toString().lastIndexOf(".")).equals(".hList")) {
            throw new IOException("File extension is wrong. hList is expected.");
        }

        Hypergraph graph = new Hypergraph();

        BufferedReader br = Files.newBufferedReader(filePath);

        br.lines().forEach(line -> {
            if (line.startsWith("#") || line.startsWith("%")) {
                return;
            }
            String[] split = line.replace(" " , "").split(":");
            long[] lAdjacentVertices = Arrays.stream(split[1].split(",")).mapToLong(Long::parseLong).toArray();

            graph.addNewEdgeByFileLine(Long.parseLong(split[0]), lAdjacentVertices);

        });

        return  graph;
    }


    /**
     * takes a given file (path) of a bipartite graph and creates a hypergraph out of it.
     * @param filePath path of the bipartite graph file
     * @return a hypergraph object
     * @throws IOException throws an exception if the file was not found
     */
    private static Hypergraph readBipartiteGraph(Path filePath) throws IOException {

        Hypergraph graph = new Hypergraph();

        BufferedReader br = Files.newBufferedReader(filePath);

        br.lines().forEach(line -> {
            if (line.startsWith("#") || line.startsWith("%")) {
                return;
            }
            String[] split = line.split(" ");

            long vertexId = Long.parseLong(split[0]);
            long edgeId = Long.parseLong((split[1]));

            Vertex vertex;
            Hyperedge edge;

            if (graph.containsEdgeId(edgeId)) {
                edge = graph.getHyperedge(edgeId);
            } else {
                edge = new Hyperedge(edgeId);
                graph.addEdgeOnly(edge);
            }

            if (graph.containsVertexId(vertexId)) {
                vertex = graph.getVertex(vertexId);
            } else {
                vertex = new Vertex(vertexId);
                graph.addVertexOnly(vertex);
            }

            vertex.addAdjacentHyperedge(edge);
            edge.addAdjacentVertex(vertex);
        });

        return  graph;
    }

}
