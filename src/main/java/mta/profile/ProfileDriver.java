package mta.profile;

import mta.MTA_combiner;
import mta.MTA_mapper;
import mta.MTA_reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Usage: ProfileDriver [-D key=val ...] &lt;input csv file|dir&gt; &lt;output dir&gt; &lt;column name&gt; [topK=20]
 *
 * Runs two MapReduce jobs on the chosen column and prints a profile report:
 *   output/summary/  - MTA_mapper/MTA_reducer summary job (counts, missing, mean/std/min/max, length range)
 *   output/freq/     - MTA_mapper/MTA_reducer frequency job (per-reducer top-K values; distinct count via counter)
 */
public class ProfileDriver extends Configured implements Tool {
    public static final String CONF_COL_INDEX = "profile.column.index";
    public static final String CONF_TOPK = "profile.topk";

    /** Input filter: skips hidden files and the .done markers written by the download script (also applied to the input dir itself, so it must accept directories). */
    public static class CsvOnly implements org.apache.hadoop.fs.PathFilter {
        static boolean accepts(Path p) {
            String n = p.getName();
            return !n.endsWith(".done") && !n.startsWith("_") && !n.startsWith(".");
        }
        @Override public boolean accept(Path p) { return accepts(p); }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: ProfileDriver <input> <output> <column> [topK]");
            return 2;
        }
        Configuration conf = getConf();
        Path input = new Path(args[0]), output = new Path(args[1]);
        String column = args[2];
        int topK = args.length > 3 ? Integer.parseInt(args[3]) : 20;

        int colIndex = resolveColumn(conf, input, column);
        conf.setInt(CONF_COL_INDEX, colIndex);
        conf.setInt(CONF_TOPK, topK);

        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) fs.delete(output, true);

        // ---- Job 1: summary
        Job j1 = Job.getInstance(conf, "profile-summary:" + column);
        j1.setJarByClass(ProfileDriver.class);
        j1.setMapperClass(MTA_mapper.SummaryMapper.class);
        j1.setCombinerClass(MTA_combiner.SummaryCombiner.class);
        j1.setReducerClass(MTA_reducer.SummaryReducer.class);
        j1.setNumReduceTasks(1);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j1, input);
        FileInputFormat.setInputPathFilter(j1, CsvOnly.class);
        FileOutputFormat.setOutputPath(j1, new Path(output, "summary"));
        if (!j1.waitForCompletion(true)) return 1;

        // ---- Job 2: frequencies
        Job j2 = Job.getInstance(conf, "profile-freq:" + column);
        j2.setJarByClass(ProfileDriver.class);
        j2.setMapperClass(MTA_mapper.FrequencyMapper.class);
        j2.setCombinerClass(MTA_combiner.FrequencyCombiner.class);
        j2.setReducerClass(MTA_reducer.TopKReducer.class);
        j2.setNumReduceTasks(conf.getInt("profile.reducers", 4));
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(LongWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(j2, input);
        FileInputFormat.setInputPathFilter(j2, CsvOnly.class);
        FileOutputFormat.setOutputPath(j2, new Path(output, "freq"));
        if (!j2.waitForCompletion(true)) return 1;
        long distinct = j2.getCounters().findCounter(MTA_reducer.FrequencyCounters.DISTINCT_VALUES).getValue();

        printReport(fs, output, column, distinct, topK);
        return 0;
    }

    /** Returns the index of {@code column} in the header of the first input file. */
    static int resolveColumn(Configuration conf, Path input, String column) throws Exception {
        List<String> cols = readHeader(conf, input);
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).trim().equalsIgnoreCase(column)) return i;
        }
        throw new IllegalArgumentException("Column '" + column + "' not in header: " + cols);
    }

    /** Parsed header line of the first (non-empty, non-hidden) input file. */
    public static List<String> readHeader(Configuration conf, Path input) throws Exception {
        FileSystem fs = input.getFileSystem(conf);
        FileStatus st = fs.getFileStatus(input);
        Path first = input;
        if (st.isDirectory()) {
            first = null;
            for (FileStatus f : fs.listStatus(input)) {
                if (f.isFile() && f.getLen() > 0 && CsvOnly.accepts(f.getPath())) {
                    first = f.getPath();
                    break;
                }
            }
            if (first == null) throw new IllegalArgumentException("No input files in " + input);
        }
        try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(first), StandardCharsets.UTF_8))) {
            String header = r.readLine();
            if (header != null && header.startsWith("\uFEFF")) header = header.substring(1);
            return CsvLine.parse(header);
        }
    }

    private void printReport(FileSystem fs, Path output, String column, long distinct, int topK) throws Exception {
        ColumnStats s = null;
        for (FileStatus f : fs.listStatus(new Path(output, "summary"))) {
            if (!f.getPath().getName().startsWith("part-")) continue;
            try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(f.getPath())))) {
                String line = r.readLine();
                if (line != null) s = ColumnStats.parse(line.split("\t")[1]);
            }
        }
        List<Object[]> top = new ArrayList<>();
        for (FileStatus f : fs.listStatus(new Path(output, "freq"))) {
            if (!f.getPath().getName().startsWith("part-")) continue;
            try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(f.getPath())))) {
                String line;
                while ((line = r.readLine()) != null) {
                    int t = line.lastIndexOf('\t');
                    top.add(new Object[]{line.substring(0, t), Long.parseLong(line.substring(t + 1))});
                }
            }
        }
        top.sort(Comparator.comparingLong((Object[] e) -> (Long) e[1]).reversed());

        StringBuilder sb = new StringBuilder();
        sb.append("\n===== Profile of column: ").append(column).append(" =====\n");
        sb.append(String.format("rows            %d%n", s.rows));
        sb.append(String.format("missing         %d (%.2f%%)%n", s.missing, 100.0 * s.missing / Math.max(1, s.rows)));
        sb.append(String.format("distinct        %d%n", distinct));
        sb.append(String.format("numeric values  %d (%.2f%% of non-missing)%n", s.numeric,
                100.0 * s.numeric / Math.max(1, s.rows - s.missing)));
        if (s.numeric > 0) {
            sb.append(String.format("min / max       %s / %s%n", fmt(s.min), fmt(s.max)));
            sb.append(String.format("mean / stddev   %.4f / %.4f%n", s.mean, s.stddev()));
            sb.append(String.format("zeros/negatives %d / %d%n", s.zeros, s.negatives));
        }
        if (s.rows > s.missing) sb.append(String.format("length min/max  %d / %d%n", s.minLen, s.maxLen));
        sb.append("top ").append(topK).append(" values:\n");
        for (int i = 0; i < Math.min(topK, top.size()); i++) {
            sb.append(String.format("  %-40s %d%n", top.get(i)[0], top.get(i)[1]));
        }
        System.out.print(sb);
        try (java.io.Writer w = new java.io.OutputStreamWriter(
                fs.create(new Path(output, "report.txt"), true), StandardCharsets.UTF_8)) {
            w.write(sb.toString());
        }
    }

    private static String fmt(double d) {
        return d == Math.rint(d) && Math.abs(d) < 1e15 ? Long.toString((long) d) : Double.toString(d);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ProfileDriver(), args));
    }
}
