package org.oxoo2a.sim4da;

import java.util.function.Supplier;

public class RandomValues {

    public RandomValues ( Supplier<Double> distributionFunction ) {
        this.distributionFunction = distributionFunction;
    }

    public double getDouble( double min_value, double max_value ) {
        double v = distributionFunction.get();
        if (v < 0 || v > 1) {
            System.err.println("Distribution function must return a value between 0 and 1");
            System.exit(-1);
        }
        return min_value + Math.random() * (max_value - min_value);
    }

    public long getLong ( long min_value, long max_value ) {
        return (long) getDouble(min_value, max_value);
    }

    public static Supplier<Double> getUniformDistribution() {
        return Math::random;
    }
    public static Supplier<Double> getNormalDistribution(double mean, double stdDev) {
        // Using the Box-Muller transform
        return () -> {
            double u, v, s;
            do {
                u = Math.random() * 2 - 1;
                v = Math.random() * 2 - 1;
                s = u * u + v * v;
            } while (s >= 1 || s == 0);
            double mul = Math.sqrt(-2.0 * Math.log(s) / s);
            return mean + stdDev * u * mul;
        };
    }

    private Supplier<Double> distributionFunction;
}