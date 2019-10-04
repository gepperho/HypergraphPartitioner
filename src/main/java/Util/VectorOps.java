package Util;

public class VectorOps {

    /**
     * calculates the sum of two vectors (element wise)
     * a + b
     *
     * @param a vector a
     * @param b vector b
     * @return result vector
     */
    public static float[] addVectors(float[] a, float[] b) {
        float[] result = new float[a.length];

        try {
            for (int i = 0; i < a.length; i++) {
                result[i] = a[i] + b[i];
            }
        } catch (IndexOutOfBoundsException e) {
            System.err.println("Dimension mismatch? a: " + a.length + " | b: " + b.length);
            throw e;
        }
        return result;
    }

    /**
     * calculates the difference of two vectors (element wise)
     * a - b
     *
     * @param a vector a
     * @param b vector b
     * @return result vector
     */
    public static float[] subtractVectors(float[] a, float[] b) {
        float[] result = new float[a.length];

        try {
            for (int i = 0; i < a.length; i++) {
                result[i] = a[i] - b[i];
            }
        } catch (IndexOutOfBoundsException e) {
            System.err.println("Dimension mismatch? a: " + a.length + " | b: " + b.length);
            throw e;
        }

        return result;
    }

    /**
     * calculates the absolute value of a vector.
     * ||vector||
     *
     * @param vector vector to be used for the calculation
     * @return length of the vector
     */
    public static float absoluteValueOfVector(float[] vector) {
        float sum = 0;
        for (float element : vector) {
            sum += Math.pow(element, 2);
        }
        return (float) Math.sqrt(sum);
    }

    /**
     * multiplies a scalar to each element of a vector
     *
     * @param scalar factor to be multiplied
     * @param vector vector to be multipied to
     * @return result vector
     */
    public static float[] multiplyScalarToVector(float scalar, float[] vector) {
        float[] result = new float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            result[i] = scalar * vector[i];
        }

        return result;
    }
}
