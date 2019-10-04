package partitioning;

import evaluation.TimeMeasurement;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import model.Hyperedge;
import model.Hypergraph;
import model.Partition;
import model.Vertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Java implementation of the HYPE partitioning algorithm.
 * https://ieeexplore.ieee.org/abstract/document/8621968
 *
 * deviation from paper:
 * - the vertices in the fringe are not removed from the vertex universe. Only after assigning a vertex to a partition
 * (core set) the vertex is remove from the vertex universe. Instead when searching for new candidates, fringe vertices
 * are filtered.
 */
public class Hype implements PartitioningStrategy {

    private final static Logger logger = LogManager.getLogger(Hype.class);

    private Hypergraph vertexUniverse;
    private TLongObjectHashMap<MatchingTuple> fringe;
    private Partition core;
    private TLongObjectHashMap<MatchingTuple> matchingCache;

    // fringe candidate set size. Value taken from the HYPE paper
    private static final int R = 2;
    // // fringe size. Value taken from the HYPE paper
    private static final int S = 10;


    @Override
    public List<Partition> partition(Hypergraph graph, int numberOfPartitions) {

        // initialize
        this.vertexUniverse = graph;
        List<Partition> cores = new ArrayList<>(numberOfPartitions);
        int initialGraphSize = graph.getOrder();


        // partition handling
        for (int i = 0; i < numberOfPartitions; i++) {

            TimeMeasurement partitionTimer = new TimeMeasurement();
            partitionTimer.startMeasurement();

            // initial partition creation
            core = new Partition(i);

            Vertex randomVertex = graph.getRandomVertex();
            core.addVertexToPartition(randomVertex);
            graph.removeVertex(randomVertex);

            cores.add(core);
            int partitionSize = calculatePartitionSize(initialGraphSize, numberOfPartitions, i);

            fringe = new TLongObjectHashMap<>();
            matchingCache = new TLongObjectHashMap<>();

            // loop to grow the partition
            while (core.getOrder() < partitionSize) {
                updateFringe();
                updateCore();

            }

            partitionTimer.endMeasurement();
            logger.debug("Partition No. " + (i + 1) + " is done after " +partitionTimer.getTimeDiferenceInSeconds() + "s." );

        }

        return cores;
    }

    @Override
    public String getStrategyName() {
        return "HYPE";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName();
    }

    private int calculatePartitionSize(int graphSize, int numberOfPartitions, int partitionId) {
        int size = graphSize / numberOfPartitions;
        int offset = graphSize % numberOfPartitions <= partitionId ? 0 : 1;
        return size + offset;
    }

    private void updateFringe() {

        ArrayList<Vertex> fringeCandidates = new ArrayList<>();

        // get adjacent edges sorted by size
        List<Hyperedge> sortedHyperedges = core.getIncompleteEdges();
         sortedHyperedges.sort((o1, o2) -> {
             if (o1.getSize() == o2.getSize()) {
                 return (int) (o1.id - o2.id);
             } else {
                 return o1.getSize() - o2.getSize();
             }
         });

         // determine r fringe candidates
        fringeCandidatesLoop: for (Hyperedge edge: sortedHyperedges) {

            // vertex loop
            TLongObjectIterator<Vertex> vertexIterator = edge.getAdjacentVerticesIterator();
            while (vertexIterator.hasNext()) {
                vertexIterator.advance();
                Vertex vertex = vertexIterator.value();

                // use -1 as dummy value. Comparison is done via the vertex.id
                if (fringe.contains(vertex.id) || core.containsVertex(vertex)) {
                    continue;
                }

                fringeCandidates.add(vertex);
                if (fringeCandidates.size() >= R) {
                    break fringeCandidatesLoop;
                }

            }
        }

        // update cache
        for (Vertex vertex: fringeCandidates) {
            if (!matchingCache.containsKey(vertex.id)) {
                // number of external neighbors
                matchingCache.put(vertex.id, new MatchingTuple(vertex, calculateExternalNeighborDegree(vertex)));
            }
        }

        // update fringe
        // F'_i from the HYPE paper, sorted list of the fringe and fringe candidates
        List<MatchingTuple> topList = new ArrayList<>(Arrays.asList(fringe.values(new MatchingTuple[fringe.size()])));
        for (Vertex candidate: fringeCandidates) {
            MatchingTuple tempTuple = matchingCache.get(candidate.id);
            topList.add(tempTuple);
        }

        // todo consider inserting the new values to avoid sorting all the time O(2n) vs O(n*log n)
        topList.sort(Comparator.comparingInt(v -> v.matching));

        // take s top elements for the fringe
        fringe.clear();
        topList.stream().limit(S).forEach(o -> fringe.put(o.vertex.id, o));

        // take a random vertex if the fringe is empty
        if (fringe.isEmpty()) {
            Vertex randomVertex = vertexUniverse.getRandomVertex();
            fringe.put(randomVertex.id, new MatchingTuple(randomVertex, calculateExternalNeighborDegree(randomVertex)));
        }
    }

    private void updateCore() {

        Optional<MatchingTuple> oVertex = Arrays.stream(fringe.values(new MatchingTuple[fringe.size()])).min(Comparator.comparingInt(o -> o.matching));
        if (oVertex.isPresent()) {
            Vertex vertex = oVertex.get().vertex;
            core.addVertexToPartition(vertex);
            fringe.remove(vertex.id);
            vertexUniverse.removeVertex(vertex);
        }

    }

    /**
     * Tuple of vertex and its external degree (matching)
     */
    private static class MatchingTuple {

        final Vertex vertex;
        final int matching;

        MatchingTuple(Vertex v, int matching) {
            this.vertex = v;
            this.matching = matching;
        }

        @Override
        public int hashCode() {
            return (int) this.vertex.id;
        }
    }

    /**
     * calculates the number of neighbors which are not part of the fringe/core.
     * Calculates d_ext(vertex, F_i) for the current Fringe (F_i) from the HYPE paper
     * @param vertex vertex which's neighbors are to be calculated
     * @return number of external neighbors
     */
    private int calculateExternalNeighborDegree(Vertex vertex) {
        // using -1 for the matching value is ok, since right now only filtering is done and the MatchingTuple Object is needed for the contains call
        return (int) vertex.getNeighbors().stream().filter(o -> !fringe.contains(o.id)).filter(o -> !core.containsVertex(o)).count();
    }
}
