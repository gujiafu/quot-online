package cn.itcast.sink;

import cn.itcast.util.HbaseUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * 通用Hbase写入类
 */
public class SinkHbase extends RichSinkFunction<List<Put>> {

    String tableName;
    public SinkHbase(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void invoke(List<Put> value, Context context) throws Exception {

        HbaseUtil.putList(tableName,value);
    }
}
