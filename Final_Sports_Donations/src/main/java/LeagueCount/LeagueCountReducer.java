package LeagueCount;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LeagueCountReducer extends Reducer <Text, LeagueDonation, Text, LeagueDonation> {

    private LeagueDonation aver = new LeagueDonation();


    protected void reduce(Text key, Iterable<LeagueDonation> values,
                          Reducer<Text, LeagueDonation, Text, LeagueDonation>.Context context)
            throws IOException, InterruptedException {

        double sum = 0;
        long count = 0;

        for (LeagueDonation ld: values){
            count += ld.getCount();
            sum += ld.getDonations();
        }
        aver.setDonations(sum);
        aver.setCount(count);


        context.write(key, aver);

    }
}