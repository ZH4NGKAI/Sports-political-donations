package MinMax;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinMaxMapper extends Mapper<LongWritable, Text, Text, MinMaxTuple > {

    MinMaxTuple minMaxTuple = new MinMaxTuple();

    protected void map(LongWritable key, Text value, Context context)
            throws
            InterruptedException, IOException {
        if(key.get()==0){  // skip headers
            return;
        }

        String[] values = value.toString().split(",");

        String party = values[6];
        double donation = Double.parseDouble(values[4]);

        minMaxTuple.setMin(donation);
        minMaxTuple.setMax(donation);
        minMaxTuple.setCount(1);

        context.write(new Text(party), minMaxTuple);
    }
}

