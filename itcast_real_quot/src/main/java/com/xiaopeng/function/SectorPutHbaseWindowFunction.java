package com.xiaopeng.function;

import com.xiaopeng.bean.SectorBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * @Date 2021
 */
public class SectorPutHbaseWindowFunction implements AllWindowFunction<SectorBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<SectorBean> values, Collector<List<Put>> out) throws Exception {

        /**
         * 开发步骤：
         * 1.新建List<Put>
         * 2.循环数据
         * 3.设置rowkey
         * 4.json数据转换
         * 5.封装put
         * 6.收集数据
         */
        List<Put> list = new ArrayList<>();
        //2.循环数据
        for (SectorBean value : values) {
            //3.设置rowkey
            String rowkey = value.getSectorCode()+value.getTradeTime();
            //4.json数据转换
            String str = JSON.toJSONString(value);
            //5.封装put
            Put put = new Put(rowkey.getBytes());
            put.addColumn("info".getBytes(),"data".getBytes(),str.getBytes());
            list.add(put);
        }
        //
        out.collect(list);

    }
}
