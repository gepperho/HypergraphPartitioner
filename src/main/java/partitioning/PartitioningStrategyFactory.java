package partitioning;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import partitioning.gravity.GravityExpansion;
import partitioning.gravity.GravityExpansionOneDimension;
import partitioning.labelPropagation.*;

import java.util.Arrays;

public class PartitioningStrategyFactory {

    public static PartitioningStrategy createStrategy(String[] args) {

        final Logger logger = LogManager.getLogger(PartitioningStrategyFactory.class);

        String strategyName = args[0];
        PartitioningStrategy strategy = null;

        switch (strategyName.toLowerCase()) {
            case "hype":
                strategy = new Hype();
                break;

            case "clp":
                // param 1: iterations, param 2: m (cluster multiplier)
                strategy = new CreditLabelPropagation(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
                break;
            case "sclp":
                // param 1: iterations
                strategy = new SplitCreditLabelPropagation(Integer.parseInt(args[1]));
                break;
            case "sclp-cc":
                // param 1: iterations
                strategy = new SplitCreditLabelPropagationCvCompensation();
                break;
            case "cc":
                boolean printOnlyLargest = false;
                if (args.length > 1) {
                    printOnlyLargest = Boolean.parseBoolean(args[1]);
                }

                strategy = new ConnectedComponents(printOnlyLargest);
                break;
            case "minmax":
                strategy = new MinMaxNB();
                break;
            case "ge":
                boolean weightedCentroid = false;
                if (args.length > 4) {
                    // there is a parameter for the weighted centroid flag
                    weightedCentroid = Boolean.parseBoolean(args[4]);
                }

                logger.info("Using weighted centroid optimization: " + weightedCentroid);

                // param 1: iterations, param 2: reexpansionRate, param 3: grid size, param 4: weighted centroid
                strategy = new GravityExpansion(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]), weightedCentroid);
                break;
            case "ge-1d":
                // param 1: iterations, param 2: reexpansionRate, param 3: grid size, param 4: weighted centroid

                weightedCentroid = false;
                if (args.length > 4) {
                    // there is a parameter for the weighted centroid flag
                    weightedCentroid = Boolean.parseBoolean(args[4]);
                }

                logger.info("Using weighted centroid optimization: " + weightedCentroid);

                strategy = new GravityExpansionOneDimension(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]), weightedCentroid);
                break;
            case "lpp":
                // param 1: iterations
                strategy = new LabelPropagationPartitioning(Integer.parseInt(args[1]));
                break;
            default:
                logger.error("Strategy parameter could not be mapped to a strategy: " + Arrays.toString(args));
                logger.error("Use --help for a list of strategies.");
                System.exit(0);
                break;
        }

        return strategy;
    }

    public static String printHelp() {

        return " MHP partitioning strategies:" +
                "\n\n" +

                // HYPE
                " hype <no further arguments>\tHYPE partitioning strategy" +
                "\n\n" +

                // Connected Component
                " cc (boolean printOnlyLargestCluster)\tConnected Components\n" +
                "\tOutput can be reduced to the largest connected component via parameter (optional)." +
                "\n\n" +

                // MinMax
                " minmax <no furhter arguments>\tminMaxNB partitioning strategy" +
                "\n\n" +

                // LPP
                " lpp <int iterations>\tLabel Propagation Partitioning Strategy\n" +
                "\tTakes the number of iterations as argument. Suggested: ~10" +
                "\n\n" +

                // CLP
                " clp <int iterations> <int cluster multiplier>\tCredit Label Propagation strategy\n" +
                "\tTakes the number of iterations as first argument. Suggested: ~10\n" +
                "\tTakes the cluster multiplier (m) as second argument. Suggested: 512" +
                "\n\n" +

                // SCLP
                " sclp <int iterations>\tSplit Credit Label Propagation strategy\n" +
                "\tTakes the number of iterations as argument. Suggested: ~10" +
                "\n\n" +

                // SCLP-cc
                " sclp-cc <int iterations>\tSplit Credit Label Propagation - Credit Value Compensation strategy\n" +
                "\tAlways performs 11 iterations" +
                "\n\n" +

                // GE
                " ge <int iterations> <int reexpansion rate> <int grid size> (boolean weightedCentroid)\tGravity Expansion strategy\n" +
                "\tTakes the number of initial iterations as first argument.\n" +
                "\tTakes the reexpansion rate as second argument.\n" +
                "\tTakes the grid size as third argument.\n" +
                "\tWeighted Cenroid optimization can be activated (optional)" +
                "\n\n" +

                // GE-1 dimension
                " ge-1d <int iterations> <int reexpansion rate> <int grid size> (boolean weightedCentroid)\tGravity Expansion strategy " +
                "with one dimension during the absorbing phase \n" +
                "\tTakes the number of initial iterations as first argument.\n" +
                "\tTakes the reexpansion rate as second argument.\n" +
                "\tTakes the grid size as third argument.\n" +
                "\tWeighted Cenroid optimization can be activated (optional)" +
                "\n\n";
    }
}
