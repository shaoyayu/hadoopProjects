package icu.shaoyayu.hadoop.mr.weather.util;

import icu.shaoyayu.hadoop.mr.weather.entity.WeatherMapOutputKeyClass;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author 邵涯语
 * @date 2020/4/18 13:00
 * @Version :
 */
public class WeatherGroupingComparator extends WritableComparator {

    public WeatherGroupingComparator(){
        super(WeatherMapOutputKeyClass.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherMapOutputKeyClass keyClass1 = (WeatherMapOutputKeyClass) a;
        WeatherMapOutputKeyClass keyClass2 = (WeatherMapOutputKeyClass) b;

        int contrast = Integer.compare(keyClass1.getYear(),keyClass2.getYear());
        //比较年
        if (contrast==0){
            return  Integer.compare(keyClass1.getMonth(),keyClass2.getMonth());
        }else {
            return contrast;
        }
    }
}
