package LeaguePartyCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LeaguePartyDonation implements Writable {
    private double donations;
    private String party;
    public LeaguePartyDonation() {}

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(donations);
        dataOutput.writeUTF(party);
    }

    public void readFields(DataInput dataInput) throws IOException {
        donations = dataInput.readDouble();
        party = dataInput.readUTF();
    }


    public double getDonations() {
        return donations;
    }

    public void setDonations(double donations) {
        this.donations = donations;
    }

    public String getParty() {
        return party;
    }

    public void setParty(String party) {
        this.party = party;
    }

    public String toString() {
        return "Party: " + party + "\tDonation Sum: " + donations;
    }
}
