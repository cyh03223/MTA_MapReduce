package mta.project;

import mta.MTA_combiner;
import mta.MTA_mapper;
import mta.MTA_reducer;
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

import java.util.List;

/**
 * Usage: AggregateDriver [-D profile.reducers=N] &lt;input csv file|dir&gt; &lt;output dir&gt;
 * Input needs (at least) the columns transit_timestamp, station_complex_id, station_complex,
 * borough, latitude, longitude, ridership, transfers - e.g. the output of ProjectDriver.
 */
public class AggregateDriver extends Configured implements Tool {
    private static final String[] KEY_COLS = {"transit_timestamp", "station_complex_id", "station_complex", "borough", "latitude", "longitude"};
    private static final String[] MEASURE_COLS = {"ridership", "transfers"};

    private static String indices(List<String> header, String[] cols) {
        StringBuilder sb = new StringBuilder();
        for (String c : cols) {
            int i = -1;
            for (int k = 0; k < header.size(); k++) if (header.get(k).trim().equalsIgnoreCase(c)) i = k;
            if (i < 0) throw new IllegalArgumentException("Column '" + c + "' not in header: " + header);
            if (sb.length() > 0) sb.append(',');
            sb.append(i);
        }
        return sb.toString();
    }

    @Override public int run(String[] args) throws Exception {
        if (args.length < 2) { System.err.println("Usage: AggregateDriver <input> <output>"); return 2; }
        Configuration conf = getConf();
        Path input = new Path(args[0]), output = new Path(args[1]);
        List<String> header = ProfileDriver.readHeader(conf, input);
        conf.set(MTA_mapper.AGG_KEY_IDX, indices(header, KEY_COLS));
        conf.set(MTA_mapper.AGG_MEASURE_IDX, indices(header, MEASURE_COLS));

        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) fs.delete(output, true);

        Job job = Job.getInstance(conf, "aggregate-hourly-station");
        job.setJarByClass(AggregateDriver.class);
        job.setMapperClass(MTA_mapper.AggregateMapper.class);
        job.setCombinerClass(MTA_combiner.AggregateCombiner.class);
        job.setReducerClass(MTA_reducer.AggregateReducer.class);
        job.setNumReduceTasks(conf.getInt("profile.reducers", 4));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputPathFilter(job, ProfileDriver.CsvOnly.class);
        FileOutputFormat.setOutputPath(job, output);
        if (!job.waitForCompletion(true)) return 1;

        Counters c = job.getCounters();
        long rin = c.findCounter(MTA_mapper.AggCounters.RIDERSHIP_IN).getValue();
        long rout = c.findCounter(MTA_mapper.AggCounters.RIDERSHIP_OUT).getValue();
        long tin = c.findCounter(MTA_mapper.AggCounters.TRANSFERS_IN).getValue();
        long tout = c.findCounter(MTA_mapper.AggCounters.TRANSFERS_OUT).getValue();
        System.out.printf("%nRows in:        %d%nGroups out:     %d%nBad rows:       %d%n",
                c.findCounter(MTA_mapper.AggCounters.ROWS_IN).getValue(),
                c.findCounter(MTA_mapper.AggCounters.GROUPS_OUT).getValue(),
                c.findCounter(MTA_mapper.AggCounters.BAD_ROWS).getValue());
        System.out.printf("Ridership in/out:  %d / %d  %s%n", rin, rout, rin == rout ? "OK" : "MISMATCH");
        System.out.printf("Transfers in/out:  %d / %d  %s%n", tin, tout, tin == tout ? "OK" : "MISMATCH");
        return (rin == rout && tin == tout) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new AggregateDriver(), args));
    }
}
