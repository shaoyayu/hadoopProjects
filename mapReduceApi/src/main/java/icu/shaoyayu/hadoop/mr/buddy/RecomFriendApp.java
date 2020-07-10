package icu.shaoyayu.hadoop.mr.buddy;

import icu.shaoyayu.hadoop.mr.buddy.mapper.RecomFriendMapper;
import icu.shaoyayu.hadoop.mr.buddy.reduce.RecomFriendReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 邵涯语
 * @date 2020/4/18 23:33
 * @Version :
 */
public class RecomFriendApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //获取配置
        Configuration configuration = new Configuration(true);

        //获取作业
        Job job = Job.getInstance(configuration);
        job.setJarByClass(RecomFriendApp.class);
        //配置
        //map环节
        job.setMapperClass(RecomFriendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //Reduce环节
        job.setReducerClass(RecomFriendReduce.class);
        //设置作业输入输出的路径

        Path inputPath = new Path("/data/friend/input/");
        FileInputFormat.setInputPaths(job,inputPath);

        Path outputPath = new Path("/data/friend/output/");
        if (outputPath.getFileSystem(configuration).exists(outputPath)){
            outputPath.getFileSystem(configuration).delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(job,outputPath);
        //提交作业
        job.waitForCompletion(true);

    }
}
