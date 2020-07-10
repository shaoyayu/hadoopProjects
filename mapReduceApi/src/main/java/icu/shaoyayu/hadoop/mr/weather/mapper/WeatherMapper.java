package icu.shaoyayu.hadoop.mr.weather.mapper;

import icu.shaoyayu.hadoop.mr.weather.entity.WeatherMapOutputKeyClass;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author 邵涯语
 * @date 2020/4/18 11:14
 * @Version :
 * 默认的输入格式化类还是TextInputFormat
 */
public class WeatherMapper extends Mapper<LongWritable, Text, WeatherMapOutputKeyClass, IntWritable> {

    WeatherMapOutputKeyClass mWeatherKeyClass = new WeatherMapOutputKeyClass();
    IntWritable mLatitudeValue =  new IntWritable();

    /**
     * 重写map的方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
        2000-01-01	16	29
        2000-01-02	14	40
        2000-01-03	23	35
        2000-01-04	18	25
        2000-01-05	14	33
        2000-01-06	14	-4
        2000-01-07	4	24
         */

        try {
            String[] sts = StringUtils.split("\t");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(sts[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            //对时间赋值
            mWeatherKeyClass.setYear(cal.get(Calendar.YEAR));
            mWeatherKeyClass.setMonth(cal.get(Calendar.MONTH)+1);
            mWeatherKeyClass.setDay(cal.get(Calendar.DAY_OF_MONTH));
            int temperature = Integer.parseInt(sts[sts.length-1].substring(0,sts[sts.length-1].length()-1));
            mWeatherKeyClass.setTemperature(temperature);
            mLatitudeValue.set(temperature);
            //输出
            context.write(mWeatherKeyClass,mLatitudeValue);
        } catch (ParseException e) {
            e.printStackTrace();
        }
//        MapTask
    }
}
