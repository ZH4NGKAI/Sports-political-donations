package OwnerCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OwnerDonation implements Writable, Comparable<OwnerDonation> {
    private double donations;

    public OwnerDonation() {}

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(donations);
    }

    public void readFields(DataInput dataInput) throws IOException {
        donations = dataInput.readDouble();
    }

    public double getDonations() {
        return donations;
    }

    public void setDonations(double donations) {
        this.donations = donations;
    }

    public String toString() {
        return "Totally Donations: " + donations;
    }

    public int compareTo(OwnerDonation o) {
        return Double.compare(this.donations, o.donations);
    }
}

