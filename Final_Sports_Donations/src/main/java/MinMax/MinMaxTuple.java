package MinMax;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxTuple implements Writable {
    private Double min;
    private Double max;
    private long count;

    public MinMaxTuple() {
    }



    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeLong(count);
    }

    public void readFields(DataInput in) throws IOException {
        min = in.readDouble();
        max = in.readDouble();
        count = in.readLong();
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

    public String toString()
    {
        return max + "\t" + min + "\t" + count;
    }

}

