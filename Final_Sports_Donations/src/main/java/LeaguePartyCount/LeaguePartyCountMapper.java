package LeaguePartyCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LeaguePartyCountMapper extends Mapper<LongWritable, Text, Text, LeaguePartyDonation> {

    private LeaguePartyDonation ld = new LeaguePartyDonation();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if(key.get()==0){  // skip headers
            return;
        }
        else{
            String values[] = value.toString().split(","); // csv
            String donation = null;
            String party = null;
            String leagues = values[2];
            try{

                donation = values[4];
                party = values[6];
                ld.setDonations(Double.parseDouble(donation));
                ld.setParty(party);

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
