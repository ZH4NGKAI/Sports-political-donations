package LeagueCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LeagueCountMapper extends Mapper<LongWritable, Text, Text, LeagueDonation> {

    private LeagueDonation ld = new LeagueDonation();

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LeagueDonation>.Context context)
            throws IOException, InterruptedException {

        if(key.get()==0){  // skip headers
            return;
        }
        else{
            String values[] = value.toString().split(","); // csv
            String donation = null;
            String leagues = values[2];
            try{

                donation = values[4];


                ld.setCount(1);
                ld.setDonations(Double.parseDouble(donation));


                // write to context
                for (String league: leagues.split(" "))
                    context.write(new Text(league), ld);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
}
