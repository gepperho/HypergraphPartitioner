package io;

import gnu.trove.iterator.TLongObjectIterator;
import model.Hyperedge;
import model.Partition;
import model.Vertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class PartitionWriter implements Runnable {

    public enum OutputFormats {vList, hList}

    private final static Logger logger = LogManager.getLogger(PartitionWriter.class);

    private Path outputDirectory;
    private Partition partition;
    private OutputFormats outputFormat;
    private String fileName;
    private int fileNumber;

    public PartitionWriter(Path outputDirectory, String fileName, int fileNumber, Partition partition, OutputFormats format) {
        this.outputDirectory = outputDirectory;
        this.partition = partition;
        this.outputFormat = format;
        this.fileName = fileName;
        this.fileNumber = fileNumber;
    }

    /**
     * creates a file holding a vertex/edge list of the partition
     */
    @Override
    public void run() {

        if (outputFormat.equals(OutputFormats.vList)) {
            // vList

            try {
                // create file
                Path vertexListPath = Paths.get(outputDirectory.toString(), String.format("%s_%d.vList", fileName, fileNumber));
                if (!vertexListPath.toFile().createNewFile()) {
                    logger.info("File already exists: " + vertexListPath.toString());
                }

                // write via buffered writer
                BufferedWriter bw = Files.newBufferedWriter(vertexListPath);


                TLongObjectIterator<Vertex> it = partition.getVertexIterator();
                while (it.hasNext()) {
                    it.advance();
                    Vertex vertex = it.value();
                    // write line of vertex list
                    // <vertexID>:<edgeId_1>,<edgeId_2>,...<edgeId_n>
                    bw.write(vertex.id + ":" + vertex.getAdjacentHyperedges().stream().map(e -> String.valueOf(e.id)).collect(Collectors.joining(",")));
                    bw.newLine();
                }

                bw.flush();
                bw.close();


            } catch (IOException e) {
                e.printStackTrace();
            }

        } else if (outputFormat.equals(OutputFormats.hList)) {
            // hList

            try {
                // create file
                Path hyperEdgeListPath = Paths.get(outputDirectory.toString(), String.format("%s_%d.hList", fileName, fileNumber));

                if (!hyperEdgeListPath.toFile().createNewFile()) {
                    logger.info("File already exists: " + hyperEdgeListPath.toString());
                }

                // write via buffered writer
                BufferedWriter bw = Files.newBufferedWriter(hyperEdgeListPath);

                TLongObjectIterator<Hyperedge> it = partition.getEdgeIterator();
                while (it.hasNext()) {
                    it.advance();
                    Hyperedge edge = it.value();
                    // write line of vertex list
                    // <edgeId>:<vertexId_1>,<vertexId_2>,...<vertexId_n>
                    StringBuilder sb = new StringBuilder();
                    TLongObjectIterator<Vertex> vertexIterator = edge.getAdjacentVerticesIterator();
                    while (vertexIterator.hasNext()) {
                        vertexIterator.advance();
                        Vertex next = vertexIterator.value();
                        sb.append(next.id);
                        sb.append(",");
                    }
                    // remove last comma
                    sb.replace(sb.lastIndexOf(","), sb.length(), "");

                    bw.write(edge.id + ":" + sb.toString());
                    bw.newLine();
                }

                bw.flush();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
