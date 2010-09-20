package tba.mr.job;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.*;
import static org.kohsuke.args4j.ExampleMode.ALL;

/**
 * Job that converts from text inputs to a sequence file of text. Useful for preprocessing small files before running through pig, etc.
 * 
 * @author rbodkin
 *
 */
public class Text2Seq extends Configured implements Tool {
    public static class Options {
        @Option(name = "-m", aliases = { "--num-mappers" }, usage = "Number of mappers")
        public Integer mappers;

        @Option(name = "-r", aliases = { "--num-reducers" }, usage = "Number of reducers")
        public Integer reducers;

        @Option(name = "-i", aliases = { "--input" }, usage = "Input file", required = true)
        public String input;

        @Option(name = "-o", aliases = { "--output" }, usage = "Output file", required = true)
        public String output;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Text2Seq(), args);
        System.exit(res);
    }

    public static class DebugIdMapper extends IdentityMapper {
        @Override
        public void configure(JobConf job) {
            try {
                String[] cmd = {"bash", "-c", "echo $PPID"};
                Process p = Runtime.getRuntime().exec(cmd);
                BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
                System.out.println(input.readLine());
                input.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public int run(String[] args) {
        Options o = new Options();
        CmdLineParser parser = new CmdLineParser(o);

        try {
            parser.parseArgument(args);
        } catch( CmdLineException e ) {
            System.err.println(e.getMessage());
            System.err.println("Usage: java text2seq [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  Example: java SampleMain"+parser.printExample(ALL));

            return 1;
        }
        if (o.input == null || o.output == null) {
            for (String arg : args) System.err.println(arg);
            System.err.println("Usage: java text2seq [options...] arguments...");
            parser.printUsage(System.err);
            return 1;
        }
        JobConf conf = new JobConf(getConf(), Text2Seq.class);
        conf.setMapperClass(DebugIdMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setCompressMapOutput(true);
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
        if (o.mappers != null)
            conf.setNumMapTasks(o.mappers);
        if (o.reducers != null) {
            conf.setNumReduceTasks(o.reducers);
        }
        conf.setNumTasksToExecutePerJvm(-1);


        FileInputFormat.setInputPaths(conf, new Path(o.input));
        FileOutputFormat.setOutputPath(conf, new Path(o.output));

        try {
            JobClient.runJob(conf);
            return 0;
        }
        catch (IOException e) {
            e.printStackTrace();
            return 1;
        }
    }

}
