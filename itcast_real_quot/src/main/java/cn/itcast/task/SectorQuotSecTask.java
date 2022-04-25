package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectFunction;
import cn.itcast.function.SectorPutHbaseWindowFunction;
import cn.itcast.function.SectorWindowFunction;
import cn.itcast.function.StockSecWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.sink.SinkHbase;
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
