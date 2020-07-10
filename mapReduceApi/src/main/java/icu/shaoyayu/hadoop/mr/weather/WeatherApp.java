package icu.shaoyayu.hadoop.mr.weather;

import icu.shaoyayu.hadoop.mr.weather.entity.WeatherMapOutputKeyClass;
import icu.shaoyayu.hadoop.mr.weather.mapper.WeatherMapper;
import icu.shaoyayu.hadoop.mr.weather.reduce.WeatherReduce;
import icu.shaoyayu.hadoop.mr.weather.util.WeatherGroupingComparator;
import icu.shaoyayu.hadoop.mr.weather.util.WeatherPartitioner;
import icu.shaoyayu.hadoop.mr.weather.util.WeatherSortComparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 邵涯语
 * @date 2020/4/17 16:41
 * @Version :
 */
public class WeatherApp {

    private static final Log LOG = LogFactory.getLog(WeatherApp.class.getName());

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //初始换配置
        Configuration configuration = new Configuration(true);

        //获取作业的实列
        Job job = Job.getInstance(configuration);
        //设置启动类
        job.setJarByClass(WeatherApp.class);

        /**
         *  在public class JobContextImpl implements JobContext 中有配置
         *  在JobContextImpl中提到获取
         *   conf.getClass(INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class);
         *  TextInputFormat.class是默认的配置，当然要是可以配置
         *  job.setInputFormatClass(MyInputFormatClass.class);
         */

        /**
         * 准备一个我们自己的mapper类 默认的是Mapper类
         */
        job.setMapperClass(WeatherMapper.class);

        /**
         * map输出的key，要实现序列化和反序列化接口
         */
        job.setMapOutputKeyClass(WeatherMapOutputKeyClass.class);

        /**
         * 设置一个输出的value的类型
         */
        job.setMapOutputValueClass(IntWritable.class);

        /**
         * 设置一个分区器
         */
        job.setPartitionerClass(WeatherPartitioner.class);

        /**
         * 设置一个排序比较累
         */
        job.setSortComparatorClass(WeatherSortComparator.class);



        /**
         * 设置一个Combiner
         * job.setCombinerClass(WeatherCombiner.class);
         */

        //==========================Reduce阶段==============================

        /**
         * 分组比较器
         */
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);

        job.setReducerClass(WeatherReduce.class);

        //设置文件输入路径
        Path InputPath = new Path("/data/weather/input/");
        FileInputFormat.setInputPaths(job,InputPath);


        //设置输出路径
        Path outputPath = new Path("data/weather/output");
        //如果路劲存在，递归删除路径
        if (outputPath.getFileSystem(configuration).exists(outputPath)){
            outputPath.getFileSystem(configuration).delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(job,outputPath);

        //设置两个Reduce的数量
        job.setNumReduceTasks(2);


        /**
         * 提交作业等待完成
         *
         */
        job.waitForCompletion(true);

    }

}
