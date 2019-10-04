package partitioning.gravity;

import Util.VectorOps;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.iterator.hash.TObjectHashIterator;
import gnu.trove.set.hash.TCustomHashSet;
import model.Hyperedge;
import model.Hypergraph;
import model.Partition;
import model.Vertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import partitioning.PartitioningStrategy;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * partitioning strategy based on the openOrd graph layout algorithm.
 * https://github.com/gephi/gephi/tree/master/modules/LayoutPlugin/src/main/java/org/gephi/layout/plugin/openord
 * <p>
 * The updates are performed 'live'. The more parallelism, the less deterministic the results might become.
 */
public class GravityExpansionOneDimension implements PartitioningStrategy {

    private final static Logger logger = LogManager.getLogger(GravityExpansionOneDimension.class);

    private final int RADIUS = 10;
    private final int TEMPERATURE = 10;

    private final int densityWeight = 2;
    private final boolean weightedCentroid;
    private int dimensions = 2;

    private int iterations;
    private int reExpansionRate;
    private int gridSize;

    // NEVER WRITE WITHOUT THE ACCORDING LOCK!
    private float[][] density;
    private Lock[][] densityLock;

    private float[][] fallOff;

    private float[] oneDimDensity;
    private Lock[] oneDimDensityLock;
    private float[] oneDimFallOff;


    public GravityExpansionOneDimension(int numberOfIterations, int reExpansionRate, int gridSize, boolean weightedCentroid) {
        this.iterations = numberOfIterations;
        this.reExpansionRate = reExpansionRate;
        this.gridSize = gridSize;
        this.weightedCentroid = weightedCentroid;
    }


    @Override
    public List<Partition> partition(Hypergraph graph, int numberOfPartitions) {

        // check if numberOfPartitions is power of 2
        if (!(numberOfPartitions > 1 && ((numberOfPartitions & (numberOfPartitions - 1)) == 0))) {
            logger.error(this.getStrategyName() + " can only handle 2^n partitions");
            return null;
        }

        int steps = (int) (Math.log(numberOfPartitions) / Math.log(2));

        return recursiveBisection(graph, steps);
    }

    /**
     * calls recursively bisections
     *
     * @param graph graph/partition to be further splitted
     * @param step  number of bisections to come
     * @return partitioning
     */
    private List<Partition> recursiveBisection(Hypergraph graph, int step) {

        List<Partition> bisection = bisection(graph);
        if (step == 1) {
            return bisection;
        } else {
            List<Partition> newList = new ArrayList<>();
            for (Partition p : bisection) {
                newList.addAll(recursiveBisection(p, step - 1));
            }

            return newList;
        }
    }

    /**
     * creates a recursiveBisection of the given graph/partitioning
     *
     * @param graph input data
     * @return list containing two partitions
     */
    private List<Partition> bisection(Hypergraph graph) {

        int numberOfPartitions = 2;

        initialize(graph);

        // LAYOUT ALGORITHM
        // iterate and move the vertices
        for (int iteration = 0; iteration < iterations; iteration++) {

            // calculate the movement of each vertex
            graph.getVertexStream().parallel().forEach(v -> this.vertexFunction(graph, v));

            logger.debug("iteration " + (iteration + 1) + " done.");
        }

        GravityObject[] gravityHoles = createGravityHoles();

        density = null;
        densityLock = null;
        fallOff = null;

        // reduce to one dimension
        initializeOneDim(gravityHoles, graph);

        // partition
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            partitions.add(new Partition(i));
            logger.debug("Gravity hole " + i + ": " + Arrays.toString(gravityHoles[i].position));
        }


        for (int i = 0; i < graph.getOrder(); i++) {
            /*
            each gravity hole 'consumes' a vertex (in a round robin fashion)
            After 'reExpand' consumptions, another round of vertex function is performed.
             */
            int currentId = i % numberOfPartitions;

            float[] gravityPosition = gravityHoles[currentId].position;

            // find closest vertex
            Optional<Vertex> nearestVertex = getClosestVertex(graph, gravityPosition);

            // consume vertex
            if (nearestVertex.isPresent()) {
                Vertex vertex = nearestVertex.get();
                ((GravityObject) vertex.getVertexData()).position = gravityPosition;
                vertex.setActive(false);
                partitions.get(currentId).addVertexToPartition(vertex);

            }


            if (i % reExpansionRate == (reExpansionRate - 1)) {
                graph.getVertexStream().parallel().filter(Vertex::isActive).forEach(v -> this.vertexFunctionOneDim(graph, v));
            }
        }

        oneDimDensityLock = null;
        oneDimDensity = null;
        oneDimFallOff = null;

        return partitions;
    }

    /**
     * initializes the grid, lock-grid, fallOff matrix and initial density
     *
     * @param graph input graph
     */
    private void initialize(Hypergraph graph) {

        dimensions = 2;
        density = new float[gridSize][gridSize];
        densityLock = new Lock[gridSize][gridSize];
        fallOff = new float[RADIUS * 2 + 1][RADIUS * 2 + 1];

        for (int i = 0; i < gridSize; i++) {
            for (int j = 0; j < gridSize; j++) {
                densityLock[i][j] = new ReentrantLock();
            }
        }

        // initialize fallOff
        for (int i = -RADIUS; i <= RADIUS; i++) {
            for (int j = -RADIUS; j <= RADIUS; j++) {
                fallOff[i + RADIUS][j + RADIUS] = ((RADIUS - Math.abs((float) i)) / RADIUS)
                        * ((RADIUS - Math.abs((float) j)) / RADIUS);
            }
        }

        graph.getVertexStream().forEach(v -> {
            v.setVertexData(new GravityObject());
            v.setActive(true);
        });

        updateDensity(((GravityObject) graph.getRandomVertex().getVertexData()).position, graph.getOrder());

        logger.debug("initialization done.");
    }

    private void initializeOneDim(GravityObject[] gravityHoles, Hypergraph graph) {

        dimensions = 1;

        oneDimDensity = new float[gridSize];
        oneDimDensityLock = new Lock[gridSize];

        for (int i = 0; i < gridSize; i++) {
            oneDimDensityLock[i] = new ReentrantLock();
        }

        // initialize fallOff
        oneDimFallOff = new float[2 * RADIUS + 1];
        for (int i = -RADIUS; i <= RADIUS; i++) {
                oneDimFallOff[i + RADIUS] = ((RADIUS - Math.abs((float) i)) / RADIUS);
        }

        float distance1 = Math.abs(gravityHoles[0].position[0] - gravityHoles[1].position[0]);
        float distance2 = Math.abs(gravityHoles[0].position[1] - gravityHoles[1].position[1]);

        if (distance1 >= distance2) {
            // reduce to first coordinate
            graph.getVertexStream().forEach(v -> {
                GravityObject go = (GravityObject) v.getVertexData();
                GravityObject x = new GravityObject();
                x.position = new float[]{go.position[0]};
                v.setVertexData(x);
            });

            for (GravityObject hole: gravityHoles) {
                hole.position = new float[] {hole.position[0]};
            }

        } else {
            // reduce to second
            graph.getVertexStream().forEach(v -> {
                GravityObject go = (GravityObject) v.getVertexData();
                GravityObject x = new GravityObject();
                x.position = new float[]{go.position[1]};
                v.setVertexData(x);
            });

            for (GravityObject hole: gravityHoles) {
                hole.position = new float[] {hole.position[1]};
            }
        }


        // update density
        graph.getVertexStream().forEach(v -> updateDensityOneDim(((GravityObject)v.getVertexData()).position, 1));

        logger.debug("one dimension initialization done.");
    }

    @Override
    public String getStrategyName() {
        return "Gravity Expansion - one dimension";
    }

    @Override
    public String getParameters() {
        return this.getStrategyName() + " - Iterations: " + iterations + ", reExpansionRate: " + reExpansionRate + ", weighted centroid: " + weightedCentroid
                + ", GridSize: " + gridSize; // + ", Temperature: " + TEMPERATURE + ", Radius: " + RADIUS + ", Density weight: " + densityWeight;
    }

    /**
     * increases the density at the given position by value
     * decreasing can be done via negative values.
     *
     * @param i     coordinate
     * @param j     coordinate
     * @param value value to be increased by (or decreased)
     */
    private void increaseDensity(int i, int j, float value) {

        if (i < 0 || j < 0 || i > gridSize - 1 || j > gridSize - 1) {
            return;
        }

        densityLock[i][j].lock();
        density[i][j] += value;
        densityLock[i][j].unlock();
    }

    private void increaseDensityOneDim(int i, float value) {

        if (i < 0 || i > gridSize - 1) {
            return;
        }

        oneDimDensityLock[i].lock();
        oneDimDensity[i] += value;
        oneDimDensityLock[i].unlock();
    }


    /**
     * vertex function. Moves a vertex to its next position.
     * Comparing a random move and a barrier jump and takes the better.
     * Also updates the density grid.
     *
     * @param vertex vertex to be moved.
     */
    private void vertexFunction(Hypergraph graph, Vertex vertex) {

        GravityObject vertexData = (GravityObject) vertex.getVertexData();
        // calculate centroid position
        float[] centroid;
        if (weightedCentroid) {
            centroid = calculateWeightedCentroid(graph, vertex);
        } else {
            centroid = calculateCentroid(graph.getLocalVertexNeighbors(vertex), vertexData.position);
        }
        // calculate its weighted sum and density
        float centroidDistance = calculateWeightedDistance(graph, vertex, centroid);
        float centroidDensity = density[mapFloatPositionToIndex(centroid[0])][mapFloatPositionToIndex(centroid[1])];
        float centroidFitting = centroidDistance + densityWeight * centroidDensity;


        // calculate a random jump
        float[] randomPosition = VectorOps.addVectors(vertexData.position, calculateRandomJump());

        // verify the random position is inside the grid
        for (int i = 0; i < randomPosition.length; i++) {
            randomPosition[i] = Float.min(gridSize - 1, randomPosition[i]);
            randomPosition[i] = Float.max(0, randomPosition[i]);
        }

        // calculate its weighted sum and density
        float randomDistance = calculateWeightedDistance(graph, vertex, randomPosition);
        float randomDensity = density[mapFloatPositionToIndex(randomPosition[0])][mapFloatPositionToIndex(randomPosition[1])];
        float randomFitting = randomDistance + densityWeight * randomDensity;


        // take the minimum
        updateDensity(vertexData.position, -1);
        if (centroidFitting < randomFitting) {
            vertexData.position = centroid;
        } else {
            vertexData.position = randomPosition;
        }
        updateDensity(vertexData.position);
    }

    private void vertexFunctionOneDim(Hypergraph graph, Vertex vertex) {

        GravityObject vertexData = (GravityObject) vertex.getVertexData();
        // calculate centroid position
        float[] centroid;
        if (weightedCentroid) {
            centroid = calculateWeightedCentroid(graph, vertex);
        } else {
            centroid = calculateCentroid(graph.getLocalVertexNeighbors(vertex), vertexData.position);
        }
        // calculate its weighted sum and density
        float centroidDistance = calculateWeightedDistance(graph, vertex, centroid);
        float centroidDensity = oneDimDensity[mapFloatPositionToIndex(centroid[0])];
        float centroidFitting = centroidDistance + densityWeight * centroidDensity;


        // calculate a random jump
        float[] randomPosition = VectorOps.addVectors(vertexData.position, calculateRandomJump());

        // verify the random position is inside the grid
        for (int i = 0; i < randomPosition.length; i++) {
            randomPosition[i] = Float.min(gridSize - 1, randomPosition[i]);
            randomPosition[i] = Float.max(0, randomPosition[i]);
        }

        // calculate its weighted sum and density
        float randomDistance = calculateWeightedDistance(graph, vertex, randomPosition);
        float randomDensity = oneDimDensity[mapFloatPositionToIndex(randomPosition[0])];
        float randomFitting = randomDistance + densityWeight * randomDensity;


        // take the minimum
        updateDensityOneDim(vertexData.position, -1);
        if (centroidFitting < randomFitting) {
            vertexData.position = centroid;
        } else {
            vertexData.position = randomPosition;
        }
        updateDensityOneDim(vertexData.position, 1);
    }


    /**
     * creates two gravity holes.
     * Takes the densest parts of the graph and draws a rectangle around it.
     * The gravity holes are placed in the middle of the short sides of the rectangle.
     *
     * @return array of gravity holes
     */
    private GravityObject[] createGravityHoles() {
        // find dense areas and draw a rectangle around...

        List<DensityObject> densityList = new ArrayList<>();
        for (int i = 0; i < gridSize; i++) {
            for (int j = 0; j < gridSize; j++) {
                if (density[i][j] > 0) {
                    densityList.add(new DensityObject(i, j, density[i][j]));
                }
            }
        }

        // TODO consider optimizing runtime
        densityList.sort(Comparator.comparingDouble(o -> o.density));
        List<DensityObject> denseSpots = new ArrayList<>();
        densityList.stream().limit((long) (densityList.size() * 0.1)).forEach(denseSpots::add);

        // rectangle data
        int xMin = gridSize;
        int xMax = 0;
        int yMin = gridSize;
        int yMax = 0;

        for (DensityObject o : denseSpots) {
            if (o.x < xMin) {
                xMin = o.x;
            } else if (o.x > xMax) {
                xMax = o.x;
            }

            if (o.y < yMin) {
                yMin = o.y;
            } else if (o.y > yMax) {
                yMax = o.y;
            }
        }

        // spawn gravity holes
        GravityObject[] gravityHoles = new GravityObject[dimensions];

        for (int i = 0; i < gravityHoles.length; i++) {
            gravityHoles[i] = new GravityObject();
        }

        gravityHoles[0].position = new float[]{xMin, yMin};
        gravityHoles[1].position = new float[]{xMax, yMax};

        return gravityHoles;
    }

    /**
     * finds the closest, active vertex to a give position.
     *
     * @param graph    the whole graph
     * @param position given position
     * @return closest vertex
     */
    private Optional<Vertex> getClosestVertex(Hypergraph graph, float[] position) {
        return graph.getVertexStream().parallel().filter(Vertex::isActive).reduce((v1, v2) -> {
            float[] p1 = ((GravityObject) v1.getVertexData()).position;
            float[] p2 = ((GravityObject) v2.getVertexData()).position;

            float distance1 = VectorOps.absoluteValueOfVector(VectorOps.subtractVectors(position, p1));
            float distance2 = VectorOps.absoluteValueOfVector(VectorOps.subtractVectors(position, p2));

            return distance1 < distance2 ? v1 : v2;
        });
    }


    /**
     * calculates the centroid of the given vertices
     *
     * @param vertices set of vertices
     * @param defaultPosition default position of the vertex. To be returned, if the vertices set is empty (use current position)
     * @return centroid
     */
    private float[] calculateCentroid(TCustomHashSet<Vertex> vertices, float[] defaultPosition) {

        TObjectHashIterator<Vertex> it = vertices.iterator();
        float[] sum = new float[dimensions];

        boolean hasNeighborOnPartition = false;

        while (it.hasNext()) {
            hasNeighborOnPartition = true;
            Vertex next = it.next();
            GravityObject d = (GravityObject) next.getVertexData();
            sum = VectorOps.addVectors(sum, d.position);
        }

        if (!hasNeighborOnPartition) {
            // vertex has no neighbors on this partition
            return defaultPosition;
        }

        return VectorOps.multiplyScalarToVector(1.0f / vertices.size(), sum);
    }

    private float[] calculateWeightedCentroid(Hypergraph graph, Vertex vertex) {

        List<Hyperedge> edges = vertex.getAdjacentHyperedges();

        // get max edge?
        int maxSize = 0;
        for (Hyperedge edge : edges) {
            if (edge.getSize() > maxSize) {
                maxSize = edge.getSize();
            }
        }

        float[] sum = new float[dimensions];
        float divider = 0f;

        for (Hyperedge edge : edges) {
            TLongObjectIterator<Vertex> it = edge.getAdjacentVerticesIterator();
            float weight = (float) maxSize / edge.getSize();

            while (it.hasNext()) {
                it.advance();
                Vertex nextVertex = it.value();

                if (nextVertex.id == vertex.id || !graph.containsVertexId(nextVertex.id)) {
                    continue;
                }

                GravityObject go = (GravityObject) nextVertex.getVertexData();
                divider += weight;
                sum = VectorOps.addVectors(sum, VectorOps.multiplyScalarToVector(weight, go.position));
            }
        }

        if (divider == 0) {
            // vertex has no neighbors on this partition
            return ((GravityObject) vertex.getVertexData()).position;
        }

        return VectorOps.multiplyScalarToVector(1f / divider, sum);
    }

    /**
     * calculates the attractive force on a vertex for a given position
     *
     * @param vertex   vertex
     * @param position position
     * @return attractive force
     */
    private float calculateWeightedDistance(Hypergraph graph, Vertex vertex, float[] position) {

        float sum = 0;
        Iterator<Hyperedge> edge_it = vertex.getAdjacentHyperedges().iterator();
        TLongObjectIterator<Vertex> vertexIt;

        while (edge_it.hasNext()) {
            Hyperedge currentEdge = edge_it.next();
            float edgeWeight = 1.0f / currentEdge.getSize();
            vertexIt = currentEdge.getAdjacentVerticesIterator();

            while (vertexIt.hasNext()) {
                vertexIt.advance();
                if (vertexIt.key() == vertex.id || !graph.containsVertexId(vertexIt.key())) {
                    continue;
                }

                Vertex neighbor = vertexIt.value();

                // calculate the deltas between the two objects positions and take the length of the resulting vector
                float[] neighborPosition = ((GravityObject) neighbor.getVertexData()).position;
                float distance = VectorOps.absoluteValueOfVector(VectorOps.subtractVectors(position, neighborPosition));
                sum += edgeWeight * Math.pow(distance, 2);

            }
        }

        return sum;
    }

    /**
     * takes a value from a position vector and maps it to the according int value on the density grid.
     *
     * @param position value from a position vector
     * @return mapped index of the density grid
     */
    private int mapFloatPositionToIndex(float position) {
        int intPosition = Math.round(position);

        // check against boundaries
        intPosition = Integer.max(0, intPosition);
        intPosition = Integer.min(gridSize - 1, intPosition);

        return intPosition;
    }

    /**
     * updates the density matrix.
     *
     * @param position   position to be updated (with surroundings)
     * @param multiplier multiplier for cumulative updates (Default 1)
     */
    private void updateDensity(float[] position, int multiplier) {

        int pos0 = mapFloatPositionToIndex(position[0]);
        int pos1 = mapFloatPositionToIndex(position[1]);

        for (int i = -RADIUS; i <= RADIUS; i++) {
            for (int j = -RADIUS; j <= RADIUS; j++) {
                float updateValue = multiplier * fallOff[i + RADIUS][j + RADIUS];
                if (updateValue != 0) { // prevent unnecessary updates, save runtime (locking etc.)
                    increaseDensity(pos0 + i, pos1 + j, updateValue);
                }
            }
        }
    }

    /**
     * updates the density matrix.
     *
     * @param position   position to be updated (with surroundings)
     * @param multiplier multiplier for cumulative updates (Default 1)
     */
    private void updateDensityOneDim(float[] position, int multiplier) {

        int pos0 = mapFloatPositionToIndex(position[0]);

        for (int i = -RADIUS; i <= RADIUS; i++) {

                float updateValue = multiplier * oneDimFallOff[i + RADIUS];
                if (updateValue != 0) { // prevent unnecessary updates, save runtime (locking etc.)
                    increaseDensityOneDim(pos0 + i, updateValue);
                }
        }
    }

    /**
     * updates the density matrix. Uses the default multiplier
     *
     * @param position position to be updated (with surroundings)
     */
    private void updateDensity(float[] position) {
        updateDensity(position, 1);
    }

    /**
     * calculates a delta with a maximal length per dimension of TEMPERATURE
     *
     * @return the delta vector
     */
    private float[] calculateRandomJump() {

        float[] delta = new float[dimensions];

        for (int i = 0; i < delta.length; i++) {

            // ensure long jumps (at least 50% of the temperature)
            float rnd = (float) Math.random();
            if (rnd < 0.5) {
                rnd = -1 + rnd;
            }
            delta[i] = TEMPERATURE * rnd;

        }

        return delta;
    }



    /**
     * wrapper class to hold coordinates
     */
    private class GravityObject {
        float[] position = new float[dimensions];

        public GravityObject() {
            for (int i = 0; i < dimensions; i++) {
                position[i] = gridSize / 2;
            }
        }
    }

    /**
     * wrapper class to hold the density of coordinates
     */
    private class DensityObject {
        private int x;
        private int y;
        private float density;

        private DensityObject(int x, int y, float density) {
            this.x = x;
            this.y = y;
            this.density = density;
        }
    }
}
