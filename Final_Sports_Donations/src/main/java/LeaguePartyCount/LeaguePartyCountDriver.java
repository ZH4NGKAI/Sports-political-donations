package LeaguePartyCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LeaguePartyCountDriver {
    public static void main(String[] args) throws Exception{
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "league_party_count");

        job.setJarByClass(LeaguePartyCountMapper.class);
        job.setMapperClass(LeaguePartyCountMapper.class);
        job.setReducerClass(LeaguePartyCountReducer.class);
        job.setCombinerClass(LeaguePartyCountReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LeaguePartyDonation.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LeaguePartyDonation.class);

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