package partitioning;

import model.Hypergraph;
import model.Partition;

import java.util.List;

public interface PartitioningStrategy {

    List<Partition> partition(Hypergraph graph, int numberOfPartitions);

    String getStrategyName();

    /**
     *
     * @return the partitioning strategy's name and parameters
     */
    String getParameters();
}
