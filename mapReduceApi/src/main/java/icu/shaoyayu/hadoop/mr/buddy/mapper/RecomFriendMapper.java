package icu.shaoyayu.hadoop.mr.buddy.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * @author 邵涯语
 * @date 2020/4/18 23:43
 * @Version :
 */
public class RecomFriendMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text mKey = new Text();
    private IntWritable mValue = new IntWritable();

    /**
     * 重写map方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //tom hello hadoop cat
        String[] sts = StringUtils.split(value.toString(),' ');

        for (int i = 1; i < sts.length; i++) {
            mKey.set(compareTwoStrings(sts[0],sts[i]));
            mValue.set(0);
            context.write(mKey,mValue);
            for (int j = i+1; j < sts.length; j++) {
                mKey.set(compareTwoStrings(sts[i],sts[j]));
                mValue.set(1);
                context.write(mKey,mValue);
            }
        }

    }

    private static String compareTwoStrings(String val1,String val2){
        if (val1.compareTo(val2) < 0){
            return val1+":"+val2;
        }
        return val2+":"+val1;
    }


}
