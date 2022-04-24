package com.xiaopeng.task;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.config.QuotConfig;
import com.xiaopeng.function.KeySelectFunction;
import com.xiaopeng.function.SectorPutHbaseWindowFunction;
import com.xiaopeng.function.SectorWindowFunction;
import com.xiaopeng.function.StockSecWindowFunction;
import com.xiaopeng.inter.ProcessDataInterface;
import com.xiaopeng.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Date 2021
 * 板块秒级行情
 */
public class SectorQuotSecTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.个股数据处理
         * 4.划分时间窗口
         * 5.秒级数据处理（新建数据写入样例类和秒级窗口函数）
         * 6.数据写入操作
         * * 封装ListPuts
         * * 数据写入
         */
        // 1.数据分组
        waterData.keyBy(new KeySelectFunction())
                .timeWindow(Time.seconds(5))
                .apply(new StockSecWindowFunction())
                .timeWindowAll(Time.seconds(5))
                .apply(new SectorWindowFunction())
                .timeWindowAll(Time.seconds(5))
                .apply(new SectorPutHbaseWindowFunction())
                .addSink(new SinkHbase(QuotConfig.config.getProperty("sector.hbase.table.name")));
    }
}
