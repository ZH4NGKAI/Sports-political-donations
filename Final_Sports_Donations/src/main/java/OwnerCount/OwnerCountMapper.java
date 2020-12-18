package OwnerCount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class OwnerCountMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

//    private OwnerDonation owner = new OwnerDonation();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if(key.get()==0){  // skip headers
            return;
        }
        else{
            String values[] = value.toString().split(","); // csv
            String donation = values[4];
            String ownerName = values[0];
            try{
//                owner.setDonations(Double.parseDouble(donation));
                // write to context

                context.write(new Text(ownerName), new DoubleWritable(Double.parseDouble(donation)));
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
}
