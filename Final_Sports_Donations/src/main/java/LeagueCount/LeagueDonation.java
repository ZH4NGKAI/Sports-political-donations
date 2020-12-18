package LeagueCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LeagueDonation implements Writable {
    private long count;
    private double donations;

    public LeagueDonation() {}

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeDouble(donations);
    }

    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readLong();
        donations = dataInput.readDouble();
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getDonations() {
        return donations;
    }

    public void setDonations(double donations) {
        this.donations = donations;
    }

    private double getAvg() {
        return this.donations/this.count;
    }

    public String toString() {
        return "Donation Times: " + count + "\tDonation Sum: " + donations + "\tDonation Avg: " + getAvg();
    }
}
