package icu.shaoyayu.hadoop.mr.buddy.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 邵涯语
 * @date 2020/4/18 23:46
 * @Version :
 */
public class RecomFriendReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable mVale = new IntWritable();

    /**
     * reduce阶段
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //tom:hello 0
        //tom:hello 1
        //tom:hello 0
        int flg = 0;
        int sum = 0;
        for (IntWritable value : values) {
            if (value.get() == 0){
                flg = 1;
            }
            sum+= value.get();
        }
        if (flg==0){
            mVale.set(sum);
            context.write(key,mVale);
        }
    }

}
