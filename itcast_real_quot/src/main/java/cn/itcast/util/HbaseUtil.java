package cn.itcast.util;

import cn.itcast.config.QuotConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Date 2020/6/9
 */
public class HbaseUtil {

    private static final Logger log = LoggerFactory.getLogger(QuotConfig.class);

    /*
      开发步骤：
      1.加载配置文件
      2.获取连接
      3.插入单列数据
      4.插入多列数据
      5.根据rowkey查询数据
      6.根据rowkey删除数据
      7.批量数据插入
   */
    static Connection connection = null;

    /**
     * 初始化链接
     */
    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", QuotConfig.config.getProperty("zookeeper.connect"));
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            log.error("hbase连接异常", e);
        }
    }

    /*
     *初始化表
     */
    public static Table getTable(String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    //3.插入单列数据
    public static void putDataByRowkey(String tableName, String family, String colName, String colVal, String rowkey) {
        Table table = getTable(tableName);
        try {
            Put put = new Put(rowkey.getBytes());
            put.addColumn(family.getBytes(), colName.getBytes(), colVal.getBytes());
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //4.插入多列数据
    public static void putMapDataByRowkey(String tableName, String family, Map<String, Object> map, String rowkey) {
        Table table = getTable(tableName);
        try {
            Put put = new Put(rowkey.getBytes());
            for (String key : map.keySet()) {
                put.addColumn(family.getBytes(), key.getBytes(), map.get(key).toString().getBytes());
            }
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //5.根据rowkey查询数据
    public static String queryByRowkey(String tableName, String family, String colName, String rowkey) {
        Table table = getTable(tableName);
        String str = null;
        try {
            Get get = new Get(rowkey.getBytes());
            Result result = table.get(get);
            byte[] bytes = result.getValue(family.getBytes(), colName.getBytes());
            if (bytes != null) {
                str = new String(bytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return str;
    }

    /**
     * 6.根据rowkey删除数据
     */
    public static void delByRowkey(String tableName, String family, String rowkey) {

        Table table = getTable(tableName);
        try {
            Delete delete = new Delete(rowkey.getBytes());
            delete.addFamily(family.getBytes());
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 7.批量数据插入
     */
    public static void putList(String tableName, List<Put> puts) {
        Table table = getTable(tableName);
        try {
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 8.根据rowkey区间查询
     * @param tableName
     * @param family
     * @param colName
     * @param startKey
     * @param endKey
     * @return
     */
    public static List<String> scanQuery(String tableName, String family, String colName, String startKey, String endKey) {

        Table table = getTable(tableName);
        Scan scan = new Scan();
        scan.setStartRow(startKey.getBytes());
        scan.setStopRow(endKey.getBytes());
        ResultScanner resultScanner = null;
        List<String> list = new ArrayList<>();
        try {
            resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                byte[] value = result.getValue(family.getBytes(), colName.getBytes());
                String str = new String(value);
                list.add(str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void main(String[] args) {
        //单列
        putDataByRowkey("test", "info", "name", "小李", "002");

        //多列
        HashMap<String, Object> map = new HashMap<>();
        map.put("name", "小红");
        map.put("age", 10);
        putMapDataByRowkey("test", "info", map, "003");

        //7.批量数据插入
        List<Put> list = new ArrayList<>();
        Put put = new Put("004".getBytes());
        put.addColumn("info".getBytes(),"name".getBytes(),"xiaoli".getBytes());
        Put put2 = new Put("005".getBytes());
        put2.addColumn("info".getBytes(),"name".getBytes(),"xiaoli2".getBytes());
        list.add(put);
        list.add(put2);
        putList("test",list);

    }

}
