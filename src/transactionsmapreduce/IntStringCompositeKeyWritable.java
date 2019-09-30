package transactionsmapreduce;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class IntStringCompositeKeyWritable implements WritableComparable<IntStringCompositeKeyWritable> {

    private int intKey;
    private String stringKey;

    public IntStringCompositeKeyWritable() {
    }

    public IntStringCompositeKeyWritable(int intKey, String stringKey) {
        this.intKey = intKey;
        this.stringKey = stringKey;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stringKey = in.readUTF();
        intKey = Integer.parseInt(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(stringKey);
        out.writeUTF(String.valueOf(intKey));
    }

    @Override
    public int compareTo(IntStringCompositeKeyWritable o) {
        return ComparisonChain.start().compare(intKey, o.intKey)
                .compare(stringKey, o.stringKey).result();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final IntStringCompositeKeyWritable other = (IntStringCompositeKeyWritable) obj;
        if (this.intKey != other.intKey) {
            return false;
        }
        return Objects.equals(this.stringKey, other.stringKey);
    }

    @Override
    public int hashCode() {
        return stringKey.hashCode() * 163 + this.intKey;
    }

    @Override
    public String toString() {
        return intKey + " " + stringKey;
    }
}