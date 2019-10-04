package evaluation;

import model.Hypergraph;
import model.Partition;

import java.util.List;

/**
 * collection of cut size metrics
 */
public class CutSize {

    /**
     * calculates the k-1 metric for a given partitioning
     *
     * @param partitioning metric input data
     * @return the k-1 metric for the partitioning
     */
    public static long calculateKMinus1Cut(List<Partition> partitioning, int graphSize) {

        long sum = partitioning.stream().mapToLong(Hypergraph::getSize).sum();

        return sum - graphSize;
    }
}
