package evaluation;

import model.Partition;

import java.util.ArrayList;
import java.util.List;

/**
 * collection of balancing metrics
 */
public class Balancing {

    /**
     * calculates the maximal imbalance between the smallest and the largest partition.
     * OR in different words: how much bigger is the largest partition compared to the smallest.
     *
     * @param partitioning partitioning, where the balance is to be measured
     * @return maximal imbalance: (max partition/min partition) - 1
     */
    public static double calculateMaxImbalance(List<Partition> partitioning) {
        int minSize = Integer.MAX_VALUE;
        int maxSize = Integer.MIN_VALUE;

        for (Partition p: partitioning) {
            if (p.getOrder() < minSize) {
                minSize = p.getOrder();
            }
            if (p.getOrder() > maxSize) {
                maxSize = p.getOrder();
            }
        }

        return (maxSize/((double) minSize)) - 1;
    }

    /**
     * calculates the standard deviation of the partition sizes
     * @param partitioning partitionings to be regarded
     * @return standard deviation
     */
    public static double calculateStandardDeviationFromBalance(List<Partition> partitioning) {
        int sum = partitioning.stream().mapToInt(Partition::getOrder).sum();
        double balanced = ((double) sum) / partitioning.size();

        ArrayList<Double> deviations = new ArrayList<>(partitioning.size());
        partitioning.forEach(p -> deviations.add(Math.pow(balanced - p.getOrder(), 2)));
        double deviationSum = deviations.stream().mapToDouble(Double::doubleValue).sum();

        return Math.sqrt(deviationSum/(partitioning.size()-1));
    }
}
