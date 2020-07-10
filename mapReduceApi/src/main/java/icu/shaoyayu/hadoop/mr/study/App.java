package icu.shaoyayu.hadoop.mr.study;

import icu.shaoyayu.hadoop.mr.study.map.MyMapper;
import icu.shaoyayu.hadoop.mr.study.reduce.MyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author shaoyayu
 *
 * 计算出每个时间段出现的行为次数最多的
 */
public class App {
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置文件
        Configuration config = new Configuration(true);
        //拿到作业
        Job job = Job.getInstance(config);
        job.setJobName("myJob_1");
        //设置启动的类
        job.setJarByClass(App.class);

        //定义一个hdfs的输入源作为输入
        Path inputPath = new Path("/user/root/user/mgs/tianmao/tianchi_mobile_recommend_train_user.csv");
        //可以定义多个数据源作为输入
        FileInputFormat.addInputPath(job,inputPath);

        //只能存在一个输出的数据源
        Path outputPath = new Path("/user/root/user/mgs/outputTianMao");
        //因为输出的路径不能存在，需要删除
        if (outputPath.getFileSystem(config).exists(outputPath)){
            outputPath.getFileSystem(config).delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(job,outputPath);

        //设置Mapper环境的类
        job.setMapperClass(MyMapper.class);
        //告诉后面的反序列化是哪个类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置Reduce环境的类
        job.setReducerClass(MyReducer.class);

        job.waitForCompletion(true);

    }
}
