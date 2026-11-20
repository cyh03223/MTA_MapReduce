package mta;

import mta.profile.ColumnStats;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiners of the MTA pipeline. A combiner runs on each mapper's output before the shuffle, so its
 * input and output types must equal the map output types, and the operation must be associative
 * and commutative (it may run zero, one or many times).
 */
public final class MTA_combiner {
    private MTA_combiner() {}

    /** Sums "ridership,transfers" partials for one group key. Same value format in and out. */
    static double[] sumPairs(Iterable<Text> vals) {
        double r = 0, t = 0;
        for (Text x : vals) {
            String s = x.toString();
            int c = s.indexOf(',');
            r += Double.parseDouble(s.substring(0, c));
            t += Double.parseDouble(s.substring(c + 1));
        }
        return new double[]{r, t};
    }

    /** Profile summary: merges partial ColumnStats (Chan's parallel update). */
    public static class SummaryCombiner extends Reducer<Text, Text, Text, Text> {
        @Override protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            ColumnStats total = new ColumnStats();
            for (Text v : vals) total.merge(ColumnStats.parse(v.toString()));
            ctx.write(key, new Text(total.toString()));
        }
    }

    /** Profile frequencies: partial counts per value. */
    public static class FrequencyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable r = new LongWritable();

        @Override protected void reduce(Text k, Iterable<LongWritable> vs, Context ctx) throws IOException, InterruptedException {
            long s = 0;
            for (LongWritable v : vs) s += v.get();
            r.set(s);
            ctx.write(k, r);
        }
    }

    /** Hourly aggregation: partial (ridership,transfers) sums per group key. */
    public static class AggregateCombiner extends Reducer<Text, Text, Text, Text> {
        @Override protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            double[] s = sumPairs(vals);
            ctx.write(key, new Text(MTA_mapper.fmt(s[0]) + "," + MTA_mapper.fmt(s[1])));
        }
    }

    /** Quality: partial row counts per hour. */
    public static class HourCountCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override protected void reduce(LongWritable k, Iterable<LongWritable> vs, Context ctx) throws IOException, InterruptedException {
            long s = 0;
            for (LongWritable v : vs) s += v.get();
            ctx.write(k, new LongWritable(s));
        }
    }
}
