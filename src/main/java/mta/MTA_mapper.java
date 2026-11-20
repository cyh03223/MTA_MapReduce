package mta;

import mta.clean.CleanRules;
import mta.profile.CsvLine;
import mta.profile.ColumnStats;
import mta.profile.ProfileDriver;
import mta.validate.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * All mappers of the MTA pipeline (one nested class per stage), plus the configuration keys,
 * counters and small helpers they share with the drivers.
 *
 * Stage -> mapper:
 *   profile   : SummaryMapper, FrequencyMapper
 *   project   : ProjectMapper      (keep selected columns)
 *   aggregate : AggregateMapper    (one row per station per hour)
 *   clean     : CleanMapper        (borough short names, numeric station id, ...)
 *   validate  : ValidateMapper     (malformed-row detection)
 *   quality   : HoursMapper, HourCountMapper (duplicates / missing hours)
 *
 * Combiners live in MTA_combiner, reducers in MTA_reducer.
 * Every mapper treats the line at byte offset 0 as the header of its input file and skips it.
 */
public final class MTA_mapper {
    private MTA_mapper() {}

    // ------------------------------------------------------------------ configuration keys
    public static final String PROJECT_INDICES = "project.column.indices";
    public static final String PROJECT_NAMES = "project.column.names";
    public static final String AGG_KEY_IDX = "agg.key.indices";          // ts,id,name,borough,lat,lon
    public static final String AGG_MEASURE_IDX = "agg.measure.indices";  // ridership,transfers
    public static final String CLEAN_COLUMNS = "clean.columns";
    public static final String CLEAN_INDICES = "clean.indices";
    public static final String CLEAN_HEADER = "clean.header";
    public static final String VALIDATE_COLUMNS = "validate.columns";
    public static final String VALIDATE_INDICES = "validate.indices";
    public static final String VALIDATE_WIDTH = "validate.width";
    public static final String VALIDATE_HEADER = "validate.header";
    public static final String QUALITY_IDX = "quality.indices";          // ts,station_id,ridership,transfers

    public static final String AGG_OUT_HEADER =
            "transit_timestamp,station_complex_id,station_complex,borough,ridership,transfers,latitude,longitude";

    // ------------------------------------------------------------------ counters
    public enum AggCounters { ROWS_IN, GROUPS_OUT, RIDERSHIP_IN, RIDERSHIP_OUT, TRANSFERS_IN, TRANSFERS_OUT, BAD_ROWS }
    public enum CleanCounters { ROWS, CHANGED_CELLS, UNMAPPED_CELLS, BAD_ROWS }
    public enum ValidateCounters { TOTAL, VALID, REJECTED }
    public enum QualityCounters { ROWS, BAD_ROWS }

    // ------------------------------------------------------------------ shared helpers
    private static final ZoneId NY = ZoneId.of("America/New_York");

    /** Hours since 1970-01-01T00 of the LOCAL timestamp, so gaps are plain integer gaps. */
    public static long hourIndex(String ts) {
        return LocalDateTime.parse(ts.trim()).toEpochSecond(ZoneOffset.UTC) / 3600;
    }

    public static String hourText(long h) {
        return LocalDateTime.ofEpochSecond(h * 3600, 0, ZoneOffset.UTC).toString();
    }

    /** True for local hours that never happen (DST spring-forward). */
    public static boolean isDstGap(long h) {
        return NY.getRules().getValidOffsets(LocalDateTime.ofEpochSecond(h * 3600, 0, ZoneOffset.UTC)).isEmpty();
    }

    public static double num(String s) {
        return (s == null || s.isEmpty()) ? 0 : Double.parseDouble(s);
    }

    /** 14.0 -> "14", 14.5 -> "14.5". */
    public static String fmt(double d) {
        return d == Math.rint(d) && Math.abs(d) < 1e15 ? Long.toString((long) d) : Double.toString(d);
    }

    static int[] ints(String csv) {
        return Arrays.stream(csv.split(",")).mapToInt(Integer::parseInt).toArray();
    }

    // ================================================================== PROFILE
    /**
     * Column summary: row count, missing, numeric moments, min/max, length range.
     * In-mapper combining: ONE record per mapper, emitted at cleanup.
     */
    public static class SummaryMapper extends Mapper<LongWritable, Text, Text, Text> {
        static final Text KEY = new Text("summary");
        private int colIndex;
        private final ColumnStats stats = new ColumnStats();

        @Override protected void setup(Context ctx) {
            colIndex = ctx.getConfiguration().getInt(ProfileDriver.CONF_COL_INDEX, -1);
        }

        @Override protected void map(LongWritable offset, Text line, Context ctx) {
            if (offset.get() == 0) return;
            stats.add(CsvLine.field(line.toString(), colIndex));
        }

        @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
            ctx.write(KEY, new Text(stats.toString()));
        }
    }

    /** Value frequencies of the chosen column: (value, 1). */
    public static class FrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private int colIndex;
        private final Text out = new Text();

        @Override protected void setup(Context ctx) {
            colIndex = ctx.getConfiguration().getInt(ProfileDriver.CONF_COL_INDEX, -1);
        }

        @Override protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            if (offset.get() == 0) return;
            String v = CsvLine.field(line.toString(), colIndex);
            if (v == null || v.isEmpty()) return; // missing values are counted by SummaryMapper
            out.set(v);
            ctx.write(out, ONE);
        }
    }

    // ================================================================== PROJECT
    /** Column projection (map-only). Each output part file starts with its own header line. */
    public static class ProjectMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private int[] idx;
        private final Text out = new Text();

        @Override protected void setup(Context ctx) throws IOException, InterruptedException {
            Configuration c = ctx.getConfiguration();
            idx = ints(c.get(PROJECT_INDICES));
            out.set(c.get(PROJECT_NAMES));
            ctx.write(NullWritable.get(), out);
        }

        @Override protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            if (offset.get() == 0) return;
            List<String> f = CsvLine.parse(line.toString());
            List<String> keep = new ArrayList<>(idx.length);
            for (int i : idx) keep.add(i < f.size() ? f.get(i).trim() : "");
            out.set(CsvLine.format(keep));
            ctx.write(NullWritable.get(), out);
        }
    }

    // ================================================================== AGGREGATE
    /** row -> (groupKey CSV of ts,id,name,borough,lat,lon ; "ridership,transfers"). */
    public static class AggregateMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int[] key, meas;
        private final Text k = new Text(), v = new Text();

        @Override protected void setup(Context ctx) {
            key = ints(ctx.getConfiguration().get(AGG_KEY_IDX));
            meas = ints(ctx.getConfiguration().get(AGG_MEASURE_IDX));
        }

        @Override protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            if (off.get() == 0) return;
            List<String> f = CsvLine.parse(line.toString());
            try {
                List<String> kf = new ArrayList<>(key.length);
                for (int i : key) kf.add(f.get(i).trim());
                double r = num(f.get(meas[0]).trim()), t = num(f.get(meas[1]).trim());
                k.set(CsvLine.format(kf));
                v.set(fmt(r) + "," + fmt(t));
                ctx.write(k, v);
                ctx.getCounter(AggCounters.ROWS_IN).increment(1);
                ctx.getCounter(AggCounters.RIDERSHIP_IN).increment((long) r);
                ctx.getCounter(AggCounters.TRANSFERS_IN).increment((long) t);
            } catch (IndexOutOfBoundsException | NumberFormatException e) {
                ctx.getCounter(AggCounters.BAD_ROWS).increment(1);
            }
        }
    }

    // ================================================================== CLEAN
    /** Applies the rule registered in CleanRules to each configured column (map-only). */
    public static class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private String[] names;
        private int[] idx;
        private final Text out = new Text();

        @Override protected void setup(Context ctx) throws IOException, InterruptedException {
            names = ctx.getConfiguration().get(CLEAN_COLUMNS).split(",");
            idx = ints(ctx.getConfiguration().get(CLEAN_INDICES));
            out.set(ctx.getConfiguration().get(CLEAN_HEADER));
            ctx.write(NullWritable.get(), out);
        }

        @Override protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            if (off.get() == 0) return;
            List<String> f = CsvLine.parse(line.toString());
            try {
                for (int k = 0; k < idx.length; k++) {
                    String before = f.get(idx[k]);
                    CleanRules.Rule rule = CleanRules.RULES.get(names[k]);
                    String after = rule.apply().apply(before);
                    if (!after.equals(before)) ctx.getCounter(CleanCounters.CHANGED_CELLS).increment(1);
                    if (!rule.known().test(before)) ctx.getCounter(CleanCounters.UNMAPPED_CELLS).increment(1);
                    f.set(idx[k], after);
                }
            } catch (IndexOutOfBoundsException e) {
                ctx.getCounter(CleanCounters.BAD_ROWS).increment(1);
                return;
            }
            out.set(CsvLine.format(f));
            ctx.write(NullWritable.get(), out);
            ctx.getCounter(CleanCounters.ROWS).increment(1);
        }
    }

    // ================================================================== VALIDATE
    /**
     * Good rows -> main output; bad rows -> "rejects/part-*" as "reasons,original_line".
     * Counters (group REASON) count failing cells per column:reason.
     */
    public static class ValidateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private String[] names;
        private int[] idx;
        private int width;
        private MultipleOutputs<NullWritable, Text> mos;
        private final Text out = new Text();

        @Override protected void setup(Context ctx) throws IOException, InterruptedException {
            names = ctx.getConfiguration().get(VALIDATE_COLUMNS).split(",");
            idx = ints(ctx.getConfiguration().get(VALIDATE_INDICES));
            width = ctx.getConfiguration().getInt(VALIDATE_WIDTH, idx.length);
            mos = new MultipleOutputs<>(ctx);
            out.set(ctx.getConfiguration().get(VALIDATE_HEADER));
            ctx.write(NullWritable.get(), out);
            mos.write("rejects", NullWritable.get(), new Text("reasons,original_line"), "rejects/part");
        }

        @Override protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            if (off.get() == 0) return;
            ctx.getCounter(ValidateCounters.TOTAL).increment(1);
            String raw = line.toString();
            List<String> f = CsvLine.parse(raw);
            List<String> reasons = new ArrayList<>();
            if (f.size() != width) {
                reasons.add("row:WRONG_FIELD_COUNT(" + f.size() + "!=" + width + ")");
                ctx.getCounter("REASON", "row:WRONG_FIELD_COUNT").increment(1);
            } else {
                for (int k = 0; k < idx.length; k++) {
                    Function<String, String> check = Schema.CHECKS.get(names[k]);
                    String r = check.apply(f.get(idx[k]));
                    if (r != null) {
                        reasons.add(names[k] + ":" + r);
                        ctx.getCounter("REASON", names[k] + ":" + r).increment(1);
                    }
                }
            }
            if (reasons.isEmpty()) {
                out.set(raw);
                ctx.write(NullWritable.get(), out);
                ctx.getCounter(ValidateCounters.VALID).increment(1);
            } else {
                mos.write("rejects", NullWritable.get(),
                        new Text(CsvLine.format(List.of(String.join(";", reasons), raw))), "rejects/part");
                ctx.getCounter(ValidateCounters.REJECTED).increment(1);
            }
        }

        @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
            mos.close();
        }
    }

    // ================================================================== QUALITY
    /** row -> (station_complex_id, "hourIndex,ridership,transfers"). */
    public static class HoursMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int[] idx;
        private final Text k = new Text(), v = new Text();

        @Override protected void setup(Context ctx) {
            idx = ints(ctx.getConfiguration().get(QUALITY_IDX));
        }

        @Override protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            if (off.get() == 0) return;
            List<String> f = CsvLine.parse(line.toString());
            try {
                long h = hourIndex(f.get(idx[0]));
                k.set(f.get(idx[1]).trim());
                v.set(h + "," + f.get(idx[2]).trim() + "," + f.get(idx[3]).trim());
                ctx.write(k, v);
                ctx.getCounter(QualityCounters.ROWS).increment(1);
            } catch (RuntimeException e) {
                ctx.getCounter(QualityCounters.BAD_ROWS).increment(1);
            }
        }
    }

    /** row -> (hourIndex, 1): how many stations reported in each hour. */
    public static class HourCountMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private int tsIdx;
        private final LongWritable k = new LongWritable();

        @Override protected void setup(Context ctx) {
            tsIdx = ints(ctx.getConfiguration().get(QUALITY_IDX))[0];
        }

        @Override protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            if (off.get() == 0) return;
            try {
                k.set(hourIndex(CsvLine.field(line.toString(), tsIdx)));
                ctx.write(k, ONE);
            } catch (RuntimeException e) { /* counted by HoursMapper */ }
        }
    }
}
