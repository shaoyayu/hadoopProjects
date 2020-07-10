package icu.shaoyayu.hadoop.mr.weather.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 邵涯语
 * @date 2020/4/17 17:36
 * @Version :
 */
public class WeatherMapOutputKeyClass implements WritableComparable<WeatherMapOutputKeyClass> {

    private int year;
    private int month;
    private int day;
    private int temperature;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    /**
     * Comparison method
     * 排序的方法，默认的是正序的排序
     * @param keyClass
     * @return
     */
    @Override
    public int compareTo(WeatherMapOutputKeyClass keyClass) {
        int sizeDetermination = Integer.compare(this.year,keyClass.year);
        if (sizeDetermination==0){
            //相等的时候判定月
            sizeDetermination = Integer.compare(this.month,keyClass.month);
            if (sizeDetermination==0){
                return Integer.compare(this.day,keyClass.day);
            }else {
                return sizeDetermination;
            }
        }
        return sizeDetermination;
    }

    /**
     * Serialization method
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.month);
        out.writeInt(this.day);
        out.writeInt(this.temperature);
    }

    /**
     * Deserialization method
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temperature = in.readInt();
    }

    @Override
    public String toString() {
        return year +"-"+ month +"-"+ day ;
    }
}
