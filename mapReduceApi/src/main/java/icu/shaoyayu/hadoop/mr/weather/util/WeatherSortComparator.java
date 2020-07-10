package icu.shaoyayu.hadoop.mr.weather.util;

import icu.shaoyayu.hadoop.mr.weather.entity.WeatherMapOutputKeyClass;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author 邵涯语
 * @date 2020/4/18 12:08
 * @Version :
 */
public class WeatherSortComparator extends WritableComparator {

    /**
     * 实例化
     */
    public WeatherSortComparator(){
        super(WeatherMapOutputKeyClass.class,true);
    }

    /**
     * 比较
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        WeatherMapOutputKeyClass keyClass1 = (WeatherMapOutputKeyClass) a;
        WeatherMapOutputKeyClass keyClass2 = (WeatherMapOutputKeyClass) b;

        int contrast = Integer.compare(keyClass1.getYear(),keyClass2.getYear());
        //比较年
        if (contrast==0){
            contrast = Integer.compare(keyClass1.getMonth(),keyClass2.getMonth());
            //比较月份
            if (contrast==0){
                //温度进行倒序比较
                return -Integer.compare(keyClass1.getTemperature(),keyClass2.getTemperature());
            }else {
                return contrast;
            }
        }else {
            return contrast;
        }

    }
}
