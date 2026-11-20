package mta.validate;

import mta.MTA_mapper;
import mta.profile.CsvLine;
import mta.profile.ProfileDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Usage: ValidateDriver [-D ...] &lt;input csv file|dir&gt; &lt;valid output dir&gt;
 * Writes good rows to &lt;valid output dir&gt; and bad rows to &lt;valid output dir&gt;_rejects.
 */
public class ValidateDriver extends Configured implements Tool {
    @Override public int run(String[] args) throws Exception {
        if (args.length < 2) { System.err.println("Usage: ValidateDriver <input> <valid output dir>"); return 2; }
        Configuration conf = getConf();
        Path input = new Path(args[0]), output = new Path(args[1]);
        Path rejects = new Path(args[1] + "_rejects");
        List<String> header = ProfileDriver.readHeader(conf, input);

        List<String> names = new ArrayList<>(), idx = new ArrayList<>();
        for (String c : Schema.CHECKS.keySet()) {
            int i = -1;
            for (int k = 0; k < header.size(); k++) if (header.get(k).trim().equalsIgnoreCase(c)) i = k;
            if (i < 0) throw new IllegalArgumentException("Column '" + c + "' not in header: " + header);
            names.add(c);
            idx.add(Integer.toString(i));
        }
        conf.set(MTA_mapper.VALIDATE_COLUMNS, String.join(",", names));
        conf.set(MTA_mapper.VALIDATE_INDICES, String.join(",", idx));
        conf.setInt(MTA_mapper.VALIDATE_WIDTH, header.size());
        conf.set(MTA_mapper.VALIDATE_HEADER, CsvLine.format(header));

        FileSystem fs = output.getFileSystem(conf);
        for (Path p : new Path[]{output, rejects}) if (fs.exists(p)) fs.delete(p, true);

        Job job = Job.getInstance(conf, "validate-rows");
        job.setJarByClass(ValidateDriver.class);
        job.setMapperClass(MTA_mapper.ValidateMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        MultipleOutputs.addNamedOutput(job, "rejects", TextOutputFormat.class, NullWritable.class, Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputPathFilter(job, ProfileDriver.CsvOnly.class);
        FileOutputFormat.setOutputPath(job, output);
        if (!job.waitForCompletion(true)) return 1;

        // Move rejects/ out of the valid dir so the valid dir contains only CSV parts.
        Path inner = new Path(output, "rejects");
        if (fs.exists(inner)) fs.rename(inner, rejects);

        long total = job.getCounters().findCounter(MTA_mapper.ValidateCounters.TOTAL).getValue();
        long valid = job.getCounters().findCounter(MTA_mapper.ValidateCounters.VALID).getValue();
        long bad = job.getCounters().findCounter(MTA_mapper.ValidateCounters.REJECTED).getValue();
        System.out.printf("%nTotal rows:  %d%nValid rows:  %d%nRejected:    %d (%.4f%%)%n", total, valid, bad, 100.0 * bad / Math.max(1, total));
        CounterGroup g = job.getCounters().getGroup("REASON");
        if (g.size() > 0) {
            System.out.println("Failing cells by column:reason");
            for (Counter c : g) System.out.printf("  %-40s %d%n", c.getName(), c.getValue());
            System.out.println("Sample rejects (" + rejects + "):");
            int shown = 0;
            for (FileStatus f : fs.listStatus(rejects)) {
                if (!f.getPath().getName().startsWith("part-")) continue;
                try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(f.getPath()), StandardCharsets.UTF_8))) {
                    r.readLine(); // header
                    String line;
                    while (shown < 10 && (line = r.readLine()) != null) { System.out.println("  " + line); shown++; }
                }
                if (shown >= 10) break;
            }
        } else {
            System.out.println("No rows failed validation.");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ValidateDriver(), args));
    }
}
