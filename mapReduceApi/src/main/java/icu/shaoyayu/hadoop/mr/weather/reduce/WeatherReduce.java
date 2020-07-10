package icu.shaoyayu.hadoop.mr.weather.reduce;


import icu.shaoyayu.hadoop.mr.weather.entity.WeatherMapOutputKeyClass;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 邵涯语
 * @date 2020/4/18 13:08
 * @Version :
 */
public class WeatherReduce extends Reducer<WeatherMapOutputKeyClass, IntWritable, Text, IntWritable> {

    Text mRKey = new Text();
    IntWritable mRValue = new IntWritable();

    /**
     * 重写Reduce方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(WeatherMapOutputKeyClass key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //values 分为每个月组的数据
        int flg = 0;
        int day = 0;
        for (IntWritable value : values) {
            if (flg==0){
                mRKey.set(key.toString());
                mRValue.set(key.getTemperature());
                flg++;
                day = key.getDay();
                context.write(mRKey,mRValue);
            }
            if (flg!=0 && day!=key.getDay()){
                mRKey.set(key.toString());
                mRValue.set(key.getTemperature());
                context.write(mRKey,mRValue);
                break;
            }
        }
    }
}
