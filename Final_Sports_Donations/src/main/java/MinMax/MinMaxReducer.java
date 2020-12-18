package MinMax;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MinMaxReducer extends Reducer<Text, MinMaxTuple, Text, StdDev> {

    StdDev result = new StdDev();
    List<Double> donations = new ArrayList<Double>();

    protected void reduce(Text key, Iterable<MinMaxTuple> values, Context context)
            throws IOException,
            InterruptedException, IOException {
        result.setMin(null);
        result.setMax(null);
        result.setCount(0);
        result.setMedian(null);
        result.setStdDev(null);
        long count = 0;
        double sum = 0.0;

        for(MinMaxTuple minMaxTuple : values)
        {
            sum += minMaxTuple.getMax();
            donations.add(minMaxTuple.getMax());

            if(result.getMin() == null || result.getMin()>minMaxTuple.getMin()){
                result.setMin(minMaxTuple.getMin());
            }

            if(result.getMax() == null || minMaxTuple.getMax()>result.getMax()){
                result.setMax(minMaxTuple.getMax());
            }

            count+= minMaxTuple.getCount();
        }

        result.setCount(count);
        result.setSum(sum);
        Collections.sort(donations);
        int len = donations.size();

        if(len%2!=0){
            result.setMedian(donations.get(len/2));
        }
        else{
            result.setMedian((donations.get((len-1)/2)+donations.get(len/2))/2.0);
        }

        double mean = sum/count;
        double stdDev = 0.0;
        for(Double donation : donations)
        {
            stdDev += (donation-mean)*(donation-mean);
        }

        result.setStdDev(Math.sqrt(stdDev /len));

        context.write(key, result);
    }
}

