package cn.itcast.function;

import cn.itcast.bean.IndexBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * @Date 2021
 * 封装写入hbase的集合数据
 */
public class IndexPutHbaseWindowFunction extends RichAllWindowFunction<IndexBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<IndexBean> values, Collector<List<Put>> out) throws Exception {

        /**
         * 1.新建list
         * 2.轮询窗口集合数据
         * 3.封装put对象
         * 4.bean转字符串
         * 5.并封装集合数据
         */
        //1.新建list
        List<Put> list = new ArrayList<>();
        //2.轮询窗口集合数据
        for(IndexBean indexBean:values){
            // 3.封装put对象
            String rowkey = indexBean.getIndexCode()+indexBean.getTradeTime();
            Put put = new Put(rowkey.getBytes());
            //4.bean转字符串
            String str = JSON.toJSONString(indexBean);
            //5.并封装集合数据
            put.addColumn("info".getBytes(),"data".getBytes(),str.getBytes());
            list.add(put);
        }
        out.collect(list);
    }
}
