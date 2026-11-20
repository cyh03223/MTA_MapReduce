package mta.quality;

import mta.MTA_combiner;
import mta.MTA_mapper;
import mta.MTA_reducer;
import mta.profile.ProfileDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Usage: QualityDriver [-D ...] &lt;input csv file|dir&gt; &lt;output dir&gt;
 * Detects duplicate (station, hour) rows and missing hours. Writes:
 *   output/stations/  per-station DUP / GAP / STATION records (Job A)
 *   output/hours/     rows per hour index (Job B)
 *   output/report.txt human-readable summary
 */
public class QualityDriver extends Configured implements Tool {
    private static int col(List<String> header, String name) {
        for (int k = 0; k < header.size(); k++) if (header.get(k).trim().equalsIgnoreCase(name)) return k;
        throw new IllegalArgumentException("Column '" + name + "' not in header: " + header);
    }

    private static List<String> lines(FileSystem fs, Path dir) throws Exception {
        List<String> out = new ArrayList<>();
        for (FileStatus f : fs.listStatus(dir)) {
            if (!f.getPath().getName().startsWith("part-")) continue;
            try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(f.getPath()), StandardCharsets.UTF_8))) {
                String l;
                while ((l = r.readLine()) != null) out.add(l);
            }
        }
        return out;
    }

    @Override public int run(String[] args) throws Exception {
        if (args.length < 2) { System.err.println("Usage: QualityDriver <input> <output>"); return 2; }
        Configuration conf = getConf();
        Path input = new Path(args[0]), output = new Path(args[1]);
        List<String> header = ProfileDriver.readHeader(conf, input);
        conf.set(MTA_mapper.QUALITY_IDX, col(header, "transit_timestamp") + "," + col(header, "station_complex_id")
                + "," + col(header, "ridership") + "," + col(header, "transfers"));
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) fs.delete(output, true);

        Job a = Job.getInstance(conf, "quality-station-hours");
        a.setJarByClass(QualityDriver.class);
        a.setMapperClass(MTA_mapper.HoursMapper.class);
        a.setReducerClass(MTA_reducer.StationReducer.class);
        a.setNumReduceTasks(conf.getInt("profile.reducers", 4));
        a.setMapOutputKeyClass(Text.class);
        a.setMapOutputValueClass(Text.class);
        a.setOutputKeyClass(NullWritable.class);
        a.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(a, input);
        FileInputFormat.setInputPathFilter(a, ProfileDriver.CsvOnly.class);
        FileOutputFormat.setOutputPath(a, new Path(output, "stations"));
        if (!a.waitForCompletion(true)) return 1;

        Job b = Job.getInstance(conf, "quality-hour-counts");
        b.setJarByClass(QualityDriver.class);
        b.setMapperClass(MTA_mapper.HourCountMapper.class);
        b.setCombinerClass(MTA_combiner.HourCountCombiner.class);
        b.setReducerClass(MTA_reducer.HourCountReducer.class);
        b.setNumReduceTasks(1);
        b.setOutputKeyClass(LongWritable.class);
        b.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(b, input);
        FileInputFormat.setInputPathFilter(b, ProfileDriver.CsvOnly.class);
        FileOutputFormat.setOutputPath(b, new Path(output, "hours"));
        if (!b.waitForCompletion(true)) return 1;

        // ---- report
        long rows = a.getCounters().findCounter(MTA_mapper.QualityCounters.ROWS).getValue();
        long bad = a.getCounters().findCounter(MTA_mapper.QualityCounters.BAD_ROWS).getValue();
        long stations = 0, stationsWithGaps = 0, stationsWithDups = 0, totalMissing = 0, totalDst = 0, dupKeys = 0, dupIdent = 0, dupConf = 0;
        List<String[]> st = new ArrayList<>(), gaps = new ArrayList<>(), dups = new ArrayList<>();
        for (String l : lines(fs, new Path(output, "stations"))) {
            String[] p = l.split("\t");
            switch (p[0]) {
                case "STATION" -> {
                    stations++;
                    long miss = Long.parseLong(p[6]), dk = Long.parseLong(p[8]);
                    if (miss > 0) stationsWithGaps++;
                    if (dk > 0) stationsWithDups++;
                    totalMissing += miss; totalDst += Long.parseLong(p[7]); dupKeys += dk;
                    st.add(p);
                }
                case "GAP" -> gaps.add(p);
                case "DUP" -> { dups.add(p); if (p[4].equals("IDENTICAL")) dupIdent++; else dupConf++; }
                default -> { }
            }
        }
        TreeMap<Long, Long> perHour = new TreeMap<>();
        for (String l : lines(fs, new Path(output, "hours"))) {
            String[] p = l.split("\t");
            perHour.put(Long.parseLong(p[0]), Long.parseLong(p[1]));
        }
        long hMin = perHour.firstKey(), hMax = perHour.lastKey(), globalMissing = 0, globalDst = 0;
        List<Long> missingHours = new ArrayList<>();
        for (long h = hMin; h <= hMax; h++) {
            if (perHour.containsKey(h)) continue;
            if (MTA_mapper.isDstGap(h)) globalDst++; else { globalMissing++; missingHours.add(h); }
        }
        List<Map.Entry<Long, Long>> lowest = new ArrayList<>(perHour.entrySet());
        lowest.sort(Map.Entry.comparingByValue());
        st.sort((x, y) -> Long.compare(Long.parseLong(y[6]), Long.parseLong(x[6])));
        gaps.sort((x, y) -> Long.compare(Long.parseLong(y[4]), Long.parseLong(x[4])));

        StringBuilder sb = new StringBuilder();
        sb.append("\n===== Data quality: duplicates and missing hours =====\n");
        sb.append(String.format("rows read                 %d (unparseable: %d)%n", rows, bad));
        sb.append(String.format("stations                  %d%n", stations));
        sb.append(String.format("hour range                %s .. %s (%d hours)%n", MTA_mapper.hourText(hMin), MTA_mapper.hourText(hMax), hMax - hMin + 1));
        sb.append("-- duplicates (same station + hour) --\n");
        sb.append(String.format("duplicate keys            %d (identical values: %d, conflicting: %d) in %d stations%n", dupKeys, dupIdent, dupConf, stationsWithDups));
        for (int i = 0; i < Math.min(10, dups.size()); i++) sb.append("  ").append(String.join("  ", dups.get(i))).append('\n');
        sb.append("-- missing hours per station (between each station's first and last row) --\n");
        sb.append(String.format("missing station-hours     %d across %d stations (%d DST spring-forward hours not counted)%n", totalMissing, stationsWithGaps, totalDst));
        sb.append("worst stations (missing hours / expected hours):\n");
        for (int i = 0; i < Math.min(10, st.size()); i++) {
            String[] p = st.get(i);
            if (Long.parseLong(p[6]) == 0) break;
            sb.append(String.format("  station %-6s missing %-7s of %-6s (active %s .. %s)%n", p[1], p[6], p[9], p[2], p[3]));
        }
        sb.append("longest gaps:\n");
        for (int i = 0; i < Math.min(10, gaps.size()); i++) {
            String[] p = gaps.get(i);
            sb.append(String.format("  station %-6s %s .. %s  (%s hours)%n", p[1], p[2], p[3], p[4]));
        }
        sb.append("-- system-wide --\n");
        sb.append(String.format("hours with no rows at all %d (+%d DST hours)%n", globalMissing, globalDst));
        for (int i = 0; i < Math.min(10, missingHours.size()); i++) sb.append("  ").append(MTA_mapper.hourText(missingHours.get(i))).append('\n');
        sb.append("hours with fewest stations reporting:\n");
        for (int i = 0; i < Math.min(5, lowest.size()); i++)
            sb.append(String.format("  %s  %d stations%n", MTA_mapper.hourText(lowest.get(i).getKey()), lowest.get(i).getValue()));
        System.out.print(sb);
        try (Writer w = new java.io.OutputStreamWriter(fs.create(new Path(output, "report.txt"), true), StandardCharsets.UTF_8)) {
            w.write(sb.toString());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new QualityDriver(), args));
    }
}
