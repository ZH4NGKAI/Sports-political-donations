package MinMax;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StdDev implements Writable {
    private Double min;
    private Double max;
    private long count;
    private Double median;
    private Double stdDev;
    private Double sum;
    public StdDev() {

    }

    public StdDev(Double min, Double max, long count, Double median, Double stdDev, Double sum) {
        this.min = min;
        this.max = max;
        this.count = count;
        this.median = median;
        this.stdDev = stdDev;
        this.sum = sum;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Double getMedian() {
        return median;
    }

    public void setMedian(Double median) {
        this.median = median;
    }

    public Double getStdDev() {
        return stdDev;
    }

    public void setStdDev(Double stdDev) {
        this.stdDev = stdDev;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(max);
        dataOutput.writeLong(count);
        dataOutput.writeDouble(median);
        dataOutput.writeDouble(stdDev);
    }

    public void readFields(DataInput dataInput) throws IOException {
        min = dataInput.readDouble();
        max = dataInput.readDouble();
        count = dataInput.readLong();
        median = dataInput.readDouble();
        stdDev = dataInput.readDouble();
    }

    public String toString(){
        return "Min: " + min + "\tMax: " + max + "\tCount: " + count + "\tSum: "+ sum +
                "\tMedian: " + median + "\tStandard Deviation: " + stdDev;
    }

}

