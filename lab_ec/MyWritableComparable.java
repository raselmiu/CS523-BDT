package labec;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritableComparable implements WritableComparable<MyWritableComparable> {

    private String stationId;
    private double temperature;
    private int year;

    public MyWritableComparable(String stationId, double temperature, int year) {
        this.stationId = stationId;
        this.temperature = temperature;
        this.year = year;
    }

    public MyWritableComparable() {
    }

    public static MyWritableComparable read(DataInput in) throws IOException {
        MyWritableComparable tuple = new MyWritableComparable();
        tuple.readFields(in);
        return tuple;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.stationId = in.readUTF();
        this.temperature = in.readDouble();
        this.year = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.stationId);
        out.writeDouble(this.temperature);
        out.writeInt(this.year);
    }

    @Override
    public int compareTo(MyWritableComparable tuple) {
        int res = this.getStationId().compareTo(tuple.getStationId());
        if (res != 0) return res;
        return -1 * Double.compare(this.getTemperature(), tuple.getTemperature());
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public String toString() {
        return stationId + " " + temperature + " " + year;
    }
}