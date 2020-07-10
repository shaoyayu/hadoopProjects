package icu.shaoyayu.hadoop.mr.study.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 邵涯语
 * @date 2020/4/10 22:26
 * @Version :
 * <KEYIN, VALUEIN, 输入类型相关的 在每一行的split的到的数据类型有关
 * KEYOUT, VALUEOUT>  输出给Reduce的数据类型
 */
public class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

    private Text word = new Text();


    /**
     * map方法会被多次调用
     * @param key   字符串的偏移量，
     * @param value 行的数据
     * @param context   上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //按照一定的方法切割字符串
        String[] split = value.toString().split(",");
        if (split.length!=6){
            return;
        }
        //取出最后一个时间小时值
        String[] times = split[split.length-1].split(" ");
        //第一行存在没有值时间
        if (times.length!=2){
            return;
        }
        //取出时间
        IntWritable time = new IntWritable(Integer.valueOf(times[1]));
        word.set(split[0]+","+split[2]);
        context.write(time, word);
    }
}
