package cn.itcast.function;

import cn.itcast.bean.StockBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * 封装写入Hbase数据
 */
public class StockPutHbaseWindowFunction implements AllWindowFunction<StockBean, List<Put>, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<StockBean> values, Collector<List<Put>> out) throws Exception {

        /**
         * 开发步骤：
         * 1.新建List<Put>
         * 2.循环数据
         * 3.设置rowkey
         * 4.json数据转换
         * 5.封装put
         * 6.收集数据
         */
        //1.新建List<Put>
        List<Put> list = new ArrayList<>();
        for (StockBean value : values) {
            //3.设置rowkey
            String rowkey = value.getSecCode() + value.getTradeTime();
            Put put = new Put(rowkey.getBytes());
            // 4.json数据转换
            put.addColumn("info".getBytes(),"data".getBytes(),JSON.toJSONString(value).getBytes());
            //5.封装put
            list.add(put);
        }
        //6.收集数据
        out.collect(list);
    }
}
