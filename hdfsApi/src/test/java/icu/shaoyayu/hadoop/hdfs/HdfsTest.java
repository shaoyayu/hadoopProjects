package icu.shaoyayu.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author 邵涯语
 * @date 2020/4/8 21:10
 * @Version :
 * hdfs的目录操作方法
 */
public class HdfsTest {

    Configuration cf = null;
    FileSystem fs = null;

    /**
     * 初始化参数
     */
    @Before
    public void conn() throws IOException {
        //初始化配置文件
        cf = new Configuration();
        //初始化hdfs客服端系统
        fs = FileSystem.get(cf);
    }

//    @Test
    public void CreateAPath() throws IOException {
        //创建一个路劲
        Path path = new Path("/mgs/apiCreate");
        //判断路径是不是存在
        if (fs.exists(path)){
            //如果存储删除路径
            System.out.println("文件路程存在，删除成功");
            fs.delete(path,true);
        }
        //创建路劲
        if (fs.mkdirs(path)){
            System.out.println("文件创建成功");
        }
    }

    /**
     * 上传文件测试
     */
    @Test
    public void uploadFiles() throws IOException {
        //创建一个文件目录
        Path path = new Path("user/mgs/tianmao/tianchi_mobile_recommend_train_user.csv");
        //
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        //拿到磁盘的文件
        InputStream inputStream = new BufferedInputStream(new FileInputStream("E:/User/shaoyayu/hadoopFiles/tianchi_mobile_recommend_train_user.csv"));

        IOUtils.copyBytes(inputStream,fsDataOutputStream,cf,true);

    }

    @Test
    public void readFile() throws IOException {
        Path path = new Path("/mgs/apiCreate/jk5172user.txt");
        FileStatus fileStatus = fs.getFileStatus(path);
        //读取文件的每个块的信息
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation fileBlockLocation : fileBlockLocations) {
            System.out.println(fileBlockLocation);
        }

    }



    /**
     * 释放资源
     */
    @After
    public void close() throws IOException {
        if (fs!=null){
            fs.close();
        }

    }
}
