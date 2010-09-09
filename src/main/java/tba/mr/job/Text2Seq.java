package tba.mr.job;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.cyclopsgroup.jcli.ArgumentProcessor;
import org.cyclopsgroup.jcli.annotation.Cli;
import org.cyclopsgroup.jcli.annotation.Option;

/**
 * Job that converts from text inputs to a sequence file of text. Useful for preprocessing small files before running through pig, etc.
 * 
 * @author rbodkin
 *
 */
public class Text2Seq extends Configured implements Tool {
    @Cli(name = "text2seq")
    public static class Options {
        @Option(name = "m", longName = "num-mappers", description = "Number of mappers")
        public Integer getMappers() {
            return mappers;
        }

        public void setMappers(Integer mappers) {
            this.mappers = mappers;
        }

        @Option(name = "r", longName = "num-reducers", description = "Number of reducers")
        public Integer getReducers() {
            return reducers;
        }

        public void setReducers(Integer mappers) {
            this.reducers = reducers;
        }

        @Option(name = "i", longName = "input", description = "Input file", required = true)
        public String getInput() {
            return input;
        }

        public void setInput(String input) {
            this.input = input;
        }

        @Option(name = "o", longName = "output", description = "Output file", required = true)
        public String getOutput() {
            return output;
        }

        public void setOutput(String output) {
            this.output = output;
        }

        public Integer mappers;
        public Integer reducers;
        public String input;
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
        ArgumentProcessor<Options> ap = ArgumentProcessor.newInstance(Options.class);
        ap.process(args, o);
        if (o.input == null || o.output == null) {
            System.err.print("Usage: text2seq ");
            try {
                ap.printHelp(new PrintWriter(System.err));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.exit(2);
        }
        JobConf conf = new JobConf(getConf(), Text2Seq.class);
        conf.setMapperClass(DebugIdMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setCompressMapOutput(true);
        if (o.mappers != null)
            conf.setNumMapTasks(o.mappers);
        if (o.reducers != null)
            conf.setNumReduceTasks(o.reducers);
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
