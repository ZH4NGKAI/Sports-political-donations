package OwnerCount;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class OwnerCountReducer extends Reducer <Text, DoubleWritable, Text, OwnerDonation> {

    private TreeMap<OwnerDonation, String> tmap;

    protected void setup(Context context)
            throws IOException,
            InterruptedException {
        tmap = new TreeMap<OwnerDonation, String>();
    }

    protected void reduce(Text key,
                          Iterable<DoubleWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        String ownerName = key.toString();
        double donation = 0;
        for (DoubleWritable d: values){
            donation += d.get();
        }
        OwnerDonation owner = new OwnerDonation();
        owner.setDonations(donation);
        tmap.put(owner, ownerName);

        if (tmap.size() > 10)
            tmap.remove(tmap.firstKey());

    }

    protected void cleanup(Context context)
            throws IOException,
            InterruptedException {

        for (Map.Entry<OwnerDonation, String> entry: tmap.entrySet()) {
            OwnerDonation owner = entry.getKey();
            String ownerName = entry.getValue();
            context.write(new Text(ownerName), owner);
        }
    }
}