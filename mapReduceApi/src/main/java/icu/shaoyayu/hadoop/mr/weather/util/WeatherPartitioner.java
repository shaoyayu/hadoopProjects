package icu.shaoyayu.hadoop.mr.weather.util;

import icu.shaoyayu.hadoop.mr.weather.entity.WeatherMapOutputKeyClass;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author 邵涯语
 * @date 2020/4/18 12:00
 * @Version :
 */
public class WeatherPartitioner extends Partitioner<WeatherMapOutputKeyClass, IntWritable> {
    @Override
    public int getPartition(WeatherMapOutputKeyClass keyClass, IntWritable intWritable, int numPartitions) {

        return keyClass.hashCode()%numPartitions;

    }
}
