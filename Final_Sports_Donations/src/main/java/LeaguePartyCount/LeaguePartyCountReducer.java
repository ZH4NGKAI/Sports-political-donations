package LeaguePartyCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class LeaguePartyCountReducer extends Reducer <Text, LeaguePartyDonation, Text, LeaguePartyDonation> {


    protected void reduce(Text key, Iterable<LeaguePartyDonation> values,
                          Context context)
            throws IOException, InterruptedException {

        Map<String, LeaguePartyDonation> map = new HashMap();

        for (LeaguePartyDonation ld: values){
            String party = ld.getParty();
            if (!map.containsKey(party)) {
                LeaguePartyDonation lpd = new LeaguePartyDonation();
                lpd.setParty(party);
                map.put(party, lpd);
            }

            double donation = map.get(party).getDonations();
            map.get(party).setDonations(donation+ld.getDonations());
        }

        for (String party: map.keySet()) {
            context.write(key, map.get(party));
        }

    }
}