package icu.shaoyayu.hadoop.hbase;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

/**
 * @author shaoyayu
 */
public class AppTest {
    //表的管理类
    HBaseAdmin admin = null;

    //表数据管理类
    HTable hTable = null;

    //表名
    String tm = "phone";
    //rowkey
    String rowKey = "rk1";

    //列族
    String family = "fm";

    /**
     * 初始化环境
     */
    @Before
    public void initializeTheEnvironment() throws IOException {
        Configuration configuration = new CompoundConfiguration();
        //连接Zookeeper配置
        configuration.set("hbase.zookeeper.quorum","hadoopNode02,hadoopNode03,hadoopNode04");
        admin = new HBaseAdmin(configuration);
        hTable = new HTable(configuration,tm.getBytes());
    }

    /**
     * 创建一张表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        //创建表名
        TableName tableName = TableName.valueOf(tm);
        HTableDescriptor ht = new HTableDescriptor(tableName);
        //添加列族
        HColumnDescriptor fam = new HColumnDescriptor(family.getBytes());
        ht.addFamily(fam);
        //判断是否已经存在
        if (admin.tableExists(tm)){
            //清除表数据
            admin.disableTable(tm);
            //删除表
            admin.deleteTable(tm);
        }
        admin.createTable(ht);

    }

    /**
     * 插入数据
     * @throws InterruptedIOException
     * @throws RetriesExhaustedWithDetailsException
     */
    @Test
    public void insertData() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        //构造参数需要一个rowKey
        Put put = new Put(rowKey.getBytes());
        put.add(family.getBytes(),"name".getBytes(),"zhangsan".getBytes());
        put.add(family.getBytes(),"age".getBytes(),"22".getBytes());
        put.add(family.getBytes(),"sex".getBytes(),"man".getBytes());
        hTable.put(put);
    }

    /**
     * 查询数据
     * @throws IOException
     */
    @Test
    public void queryData() throws IOException {
        Get get = new Get(rowKey.getBytes());
        //添加一个在服务器上的过滤操作，减少网络i/o
        get.addColumn(family.getBytes(),"name".getBytes());
        Result result = hTable.get(get);
        Cell cell = result.getColumnLatestCell(family.getBytes(), "name".getBytes());
        byte[] bytes = CellUtil.cloneValue(cell);
        String name = Bytes.toString(bytes);
        System.out.println(name);
    }

    /**
     * 查询整张表
     * @throws IOException
     */
    @Test
    public void queryTheEntireTable() throws IOException {
        Scan scan = new Scan();
        //可以通过rowkey来限定Scan查询传来的数据量
//        scan.setStartRow();
//        scan.setStopRow();
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            Cell cell = result.getColumnLatestCell(family.getBytes(), "name".getBytes());
            byte[] bytes = CellUtil.cloneValue(cell);
            String name = Bytes.toString(bytes);
            System.out.println(name);
        }
    }

    @After
    public void destory(){
        if (admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
