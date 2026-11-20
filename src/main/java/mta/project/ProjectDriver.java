package mta.project;

import mta.MTA_mapper;
import mta.profile.CsvLine;
import mta.profile.ProfileDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Usage: ProjectDriver [-D key=val ...] &lt;input csv file|dir&gt; &lt;output dir&gt; [col1,col2,...]
 * Default columns: the 8 MTA columns below.
 */
public class ProjectDriver extends Configured implements Tool {
    static final String DEFAULT_COLUMNS =
            "transit_timestamp,station_complex_id,station_complex,borough,ridership,transfers,latitude,longitude";

    @Override public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ProjectDriver <input> <output> [col1,col2,...]");
            return 2;
        }
        Configuration conf = getConf();
        Path input = new Path(args[0]), output = new Path(args[1]);
        String[] wanted = (args.length > 2 ? args[2] : DEFAULT_COLUMNS).split(",");

        List<String> header = ProfileDriver.readHeader(conf, input);
        List<String> idx = new ArrayList<>(), names = new ArrayList<>();
        for (String w : wanted) {
            int i = -1;
            for (int k = 0; k < header.size(); k++) if (header.get(k).trim().equalsIgnoreCase(w.trim())) i = k;
            if (i < 0) throw new IllegalArgumentException("Column '" + w + "' not in header: " + header);
            idx.add(Integer.toString(i));
            names.add(header.get(i).trim());
        }
        conf.set(MTA_mapper.PROJECT_INDICES, String.join(",", idx));
        conf.set(MTA_mapper.PROJECT_NAMES, CsvLine.format(names));

        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) fs.delete(output, true);

        Job job = Job.getInstance(conf, "project-columns");
        job.setJarByClass(ProjectDriver.class);
        job.setMapperClass(MTA_mapper.ProjectMapper.class);
        job.setNumReduceTasks(0); // map-only
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputPathFilter(job, ProfileDriver.CsvOnly.class);
        FileOutputFormat.setOutputPath(job, output);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ProjectDriver(), args));
    }
}
