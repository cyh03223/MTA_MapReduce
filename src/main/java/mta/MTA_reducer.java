package mta;

import mta.profile.ColumnStats;
import mta.profile.CsvLine;
import mta.profile.ProfileDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Reducers of the MTA pipeline. (The project, clean and validate stages are map-only and have no reducer.)
 *
 *   profile   : SummaryReducer, TopKReducer
 *   aggregate : AggregateReducer
 *   quality   : StationReducer, HourCountReducer
 */
public final class MTA_reducer {
    private MTA_reducer() {}

    public enum FrequencyCounters { DISTINCT_VALUES }

    // ================================================================== PROFILE
    /** Merges the per-mapper ColumnStats into the final column summary (single reducer). */
    public static class SummaryReducer extends Reducer<Text, Text, Text, Text> {
        @Override protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            ColumnStats total = new ColumnStats();
            for (Text v : vals) total.merge(ColumnStats.parse(v.toString()));
            ctx.write(key, new Text(total.toString()));
        }
    }

    /**
     * Sums value counts, counts distinct values (counter DISTINCT_VALUES) and keeps this reducer's top-K.
     * Keys are hash-partitioned, so the driver merges the per-reducer top-K lists into the global top-K.
     */
    public static class TopKReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private int k;
        private final PriorityQueue<Object[]> heap =
                new PriorityQueue<>((a, b) -> Long.compare((Long) a[1], (Long) b[1])); // min-heap

        @Override protected void setup(Context ctx) {
            k = ctx.getConfiguration().getInt(ProfileDriver.CONF_TOPK, 20);
        }

        @Override protected void reduce(Text key, Iterable<LongWritable> vals, Context ctx) {
            long s = 0;
            for (LongWritable v : vals) s += v.get();
            ctx.getCounter(FrequencyCounters.DISTINCT_VALUES).increment(1);
            heap.add(new Object[]{key.toString(), s});
            if (heap.size() > k) heap.poll();
        }

        @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
            for (Object[] e : heap) ctx.write(new Text((String) e[0]), new LongWritable((Long) e[1]));
        }
    }

    // ================================================================== AGGREGATE
    /**
     * Final sums per group -> one CSV line, columns in the order
     * timestamp,station_complex_id,station_complex,borough,ridership,transfers,latitude,longitude.
     */
    public static class AggregateReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override protected void setup(Context ctx) throws IOException, InterruptedException {
            ctx.write(NullWritable.get(), new Text(MTA_mapper.AGG_OUT_HEADER)); // header in every part file
        }

        @Override protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            double[] s = MTA_combiner.sumPairs(vals);
            List<String> k = CsvLine.parse(key.toString()); // ts,id,name,borough,lat,lon
            String out = CsvLine.format(Arrays.asList(k.get(0), k.get(1), k.get(2), k.get(3),
                    MTA_mapper.fmt(s[0]), MTA_mapper.fmt(s[1]), k.get(4), k.get(5)));
            ctx.write(NullWritable.get(), new Text(out));
            ctx.getCounter(MTA_mapper.AggCounters.GROUPS_OUT).increment(1);
            ctx.getCounter(MTA_mapper.AggCounters.RIDERSHIP_OUT).increment((long) s[0]);
            ctx.getCounter(MTA_mapper.AggCounters.TRANSFERS_OUT).increment((long) s[1]);
        }
    }

    // ================================================================== QUALITY
    /**
     * Per-station duplicate and missing-hour detection. Receives all hours of one station, sorted:
     *   DUP     station, hour, copies, IDENTICAL|CONFLICTING   - same station+hour more than once
     *   GAP     station, firstMissingHour, lastMissingHour, count - hours missing between first and last row
     *   STATION station, firstHour, lastHour, rows, distinctHours, missingHours, dstSkipped, dupKeys, expectedHours
     * Hours that do not exist in New York local time (spring-forward 02:00) are not counted as missing.
     */
    public static class StationReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override protected void reduce(Text station, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            List<String[]> rows = new ArrayList<>();
            for (Text t : vals) rows.add(t.toString().split(",", 3));
            rows.sort((a, b) -> Long.compare(Long.parseLong(a[0]), Long.parseLong(b[0])));

            long first = Long.parseLong(rows.get(0)[0]), last = Long.parseLong(rows.get(rows.size() - 1)[0]);
            long distinct = 0, missing = 0, dstSkipped = 0, dupKeys = 0;
            long prev = Long.MIN_VALUE;
            int i = 0;
            while (i < rows.size()) {
                long h = Long.parseLong(rows.get(i)[0]);
                int j = i;
                boolean conflicting = false;
                while (j < rows.size() && Long.parseLong(rows.get(j)[0]) == h) {
                    if (!rows.get(j)[1].equals(rows.get(i)[1]) || !rows.get(j)[2].equals(rows.get(i)[2])) conflicting = true;
                    j++;
                }
                if (j - i > 1) {
                    dupKeys++;
                    ctx.write(NullWritable.get(), new Text(String.join("\t", "DUP", station.toString(), MTA_mapper.hourText(h),
                            Integer.toString(j - i), conflicting ? "CONFLICTING" : "IDENTICAL")));
                }
                distinct++;
                if (prev != Long.MIN_VALUE && h - prev > 1) {
                    long gapMissing = 0, firstMissing = -1, lastMissing = -1;
                    for (long m = prev + 1; m < h; m++) {
                        if (MTA_mapper.isDstGap(m)) { dstSkipped++; continue; }
                        gapMissing++;
                        if (firstMissing < 0) firstMissing = m;
                        lastMissing = m;
                    }
                    if (gapMissing > 0) {
                        missing += gapMissing;
                        ctx.write(NullWritable.get(), new Text(String.join("\t", "GAP", station.toString(),
                                MTA_mapper.hourText(firstMissing), MTA_mapper.hourText(lastMissing), Long.toString(gapMissing))));
                    }
                }
                prev = h;
                i = j;
            }
            ctx.write(NullWritable.get(), new Text(String.join("\t", "STATION", station.toString(),
                    MTA_mapper.hourText(first), MTA_mapper.hourText(last), Integer.toString(rows.size()), Long.toString(distinct),
                    Long.toString(missing), Long.toString(dstSkipped), Long.toString(dupKeys), Long.toString(last - first + 1))));
        }
    }

    /** Rows per hour (how many stations reported in each hour). */
    public static class HourCountReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override protected void reduce(LongWritable k, Iterable<LongWritable> vs, Context ctx) throws IOException, InterruptedException {
            long s = 0;
            for (LongWritable v : vs) s += v.get();
            ctx.write(k, new LongWritable(s));
        }
    }
}
