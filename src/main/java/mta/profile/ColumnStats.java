package mta.profile;

/**
 * Mergeable summary of one column. Numeric moments use Welford/Chan updates so partial
 * results from different mappers/combiners can be merged without catastrophic cancellation.
 * Serialised as a tab-free comma string so it can travel as a Hadoop Text value.
 */
public class ColumnStats {
    public long rows;          // every data row seen
    public long missing;       // empty / absent value
    public long numeric;       // values parseable as double
    public double mean;        // over numeric values
    public double m2;          // sum of squared deviations
    public double min = Double.POSITIVE_INFINITY;
    public double max = Double.NEGATIVE_INFINITY;
    public long zeros;
    public long negatives;
    public int minLen = Integer.MAX_VALUE; // over non-missing values
    public int maxLen = 0;

    public void add(String v) {
        rows++;
        if (v == null || v.isEmpty()) {
            missing++;
            return;
        }
        minLen = Math.min(minLen, v.length());
        maxLen = Math.max(maxLen, v.length());
        double d;
        try {
            d = Double.parseDouble(v);
            if (Double.isNaN(d) || Double.isInfinite(d)) return;
        } catch (NumberFormatException e) {
            return;
        }
        numeric++;
        double delta = d - mean;
        mean += delta / numeric;
        m2 += delta * (d - mean);
        if (d < min) min = d;
        if (d > max) max = d;
        if (d == 0) zeros++;
        if (d < 0) negatives++;
    }

    public void merge(ColumnStats o) {
        rows += o.rows;
        missing += o.missing;
        zeros += o.zeros;
        negatives += o.negatives;
        minLen = Math.min(minLen, o.minLen);
        maxLen = Math.max(maxLen, o.maxLen);
        min = Math.min(min, o.min);
        max = Math.max(max, o.max);
        if (o.numeric > 0) {
            long n = numeric + o.numeric;
            double delta = o.mean - mean;
            m2 += o.m2 + delta * delta * numeric * o.numeric / n;
            mean += delta * o.numeric / n;
            numeric = n;
        }
    }

    public double variance() { return numeric > 1 ? m2 / (numeric - 1) : 0; }
    public double stddev()   { return Math.sqrt(variance()); }

    @Override public String toString() {
        return String.join(",", Long.toString(rows), Long.toString(missing), Long.toString(numeric),
                Double.toString(mean), Double.toString(m2), Double.toString(min), Double.toString(max),
                Long.toString(zeros), Long.toString(negatives), Integer.toString(minLen), Integer.toString(maxLen));
    }

    public static ColumnStats parse(String s) {
        String[] p = s.split(",");
        ColumnStats c = new ColumnStats();
        c.rows = Long.parseLong(p[0]);
        c.missing = Long.parseLong(p[1]);
        c.numeric = Long.parseLong(p[2]);
        c.mean = Double.parseDouble(p[3]);
        c.m2 = Double.parseDouble(p[4]);
        c.min = Double.parseDouble(p[5]);
        c.max = Double.parseDouble(p[6]);
        c.zeros = Long.parseLong(p[7]);
        c.negatives = Long.parseLong(p[8]);
        c.minLen = Integer.parseInt(p[9]);
        c.maxLen = Integer.parseInt(p[10]);
        return c;
    }
}
