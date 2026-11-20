package mta.clean;

import mta.MTA_mapper;
import mta.profile.CsvLine;
import mta.profile.ProfileDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Usage: CleanDriver [-D ...] &lt;input csv file|dir&gt; &lt;output dir&gt; [col1,col2,...]
 * Default: every column that has a rule in CleanRules and exists in the input header.
 */
public class CleanDriver extends Configured implements Tool {
    @Override public int run(String[] args) throws Exception {
        if (args.length < 2) { System.err.println("Usage: CleanDriver <input> <output> [columns]"); return 2; }
        Configuration conf = getConf();
        Path input = new Path(args[0]), output = new Path(args[1]);
        List<String> header = ProfileDriver.readHeader(conf, input);

        List<String> wanted = new ArrayList<>();
        if (args.length > 2) for (String s : args[2].split(",")) wanted.add(s.trim().toLowerCase());
        else wanted.addAll(CleanRules.RULES.keySet());

        List<String> names = new ArrayList<>(), idx = new ArrayList<>();
        for (String w : wanted) {
            if (!CleanRules.RULES.containsKey(w)) throw new IllegalArgumentException("No cleaning rule for column '" + w + "'");
            int i = -1;
            for (int k = 0; k < header.size(); k++) if (header.get(k).trim().equalsIgnoreCase(w)) i = k;
            if (i < 0) { System.err.println("Skipping '" + w + "': not in input header"); continue; }
            names.add(w);
            idx.add(Integer.toString(i));
        }
        if (names.isEmpty()) throw new IllegalArgumentException("Nothing to clean");
        conf.set(MTA_mapper.CLEAN_COLUMNS, String.join(",", names));
        conf.set(MTA_mapper.CLEAN_INDICES, String.join(",", idx));
        conf.set(MTA_mapper.CLEAN_HEADER, CsvLine.format(header));

        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) fs.delete(output, true);

        Job job = Job.getInstance(conf, "clean:" + String.join(",", names));
        job.setJarByClass(CleanDriver.class);
        job.setMapperClass(MTA_mapper.CleanMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputPathFilter(job, ProfileDriver.CsvOnly.class);
        FileOutputFormat.setOutputPath(job, output);
        if (!job.waitForCompletion(true)) return 1;

        Counters c = job.getCounters();
        System.out.printf("%nCleaned columns: %s%nRows written:    %d%nCells changed:   %d%nUnmapped cells: %d%nBad rows:        %d%n",
                names, c.findCounter(MTA_mapper.CleanCounters.ROWS).getValue(),
                c.findCounter(MTA_mapper.CleanCounters.CHANGED_CELLS).getValue(),
                c.findCounter(MTA_mapper.CleanCounters.UNMAPPED_CELLS).getValue(),
                c.findCounter(MTA_mapper.CleanCounters.BAD_ROWS).getValue());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new CleanDriver(), args));
    }
}
