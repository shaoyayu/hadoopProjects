package icu.shaoyayu.hadoop.mr.study.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 邵涯语
 * @date 2020/4/10 22:27
 * @Version :
 * <Text, IntWritable, 这个地方的输入来自map阶段的输出
 * Text, IntWritable>   自定义的输出类型
 */
public class MyReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text val : values) {
            sum = sum+1;
        }
        result.set(sum);
        context.write(key, result);
    }
//    ReduceTask
}
