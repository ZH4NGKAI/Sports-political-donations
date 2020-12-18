package LeagueCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LeagueCountDriver {
    public static void main(String[] args) throws Exception{
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "league_count");

        job.setJarByClass(LeagueCountMapper.class);
        job.setMapperClass(LeagueCountMapper.class);
        job.setReducerClass(LeagueCountReducer.class);
        job.setCombinerClass(LeagueCountReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LeagueDonation.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LeagueDonation.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(output)){
            hdfs.delete(output, true);
        }
        job.waitForCompletion(true);
    }
}