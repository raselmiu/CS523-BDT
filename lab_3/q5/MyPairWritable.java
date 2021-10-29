package lab3.q5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyPairWritable implements Writable {

    private double sum;
    private int count;

    public MyPairWritable(double sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public MyPairWritable() {
    }

    public static MyPairWritable read(DataInput in) throws IOException {
        MyPairWritable pair = new MyPairWritable();
        pair.readFields(in);
        return pair;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeInt(count);

    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}