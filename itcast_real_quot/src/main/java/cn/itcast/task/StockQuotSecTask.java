package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectFunction;
import cn.itcast.function.StockPutHbaseWindowFunction;
import cn.itcast.function.StockSecWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 个股秒级行情业务(5s)
 */
public class StockQuotSecTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.新建个股数据写入bean对象
         * 4.秒级窗口函数业务处理
         * 5.数据写入操作
         *   * 封装ListPuts
         *   * 数据写入
         */
        //1.数据分组
        waterData.keyBy(new KeySelectFunction())
                // 2.划分时间窗口
                .timeWindow(Time.seconds(5)) //每5s一条最新数据
                .apply(new StockSecWindowFunction())
                //取到了最新一条数据
                .timeWindowAll(Time.seconds(5)) //不对数据分组就是把所有数据收集在一个窗口里
                .apply(new StockPutHbaseWindowFunction())//封装ListPuts
                //数据写入
                .addSink(new SinkHbase(QuotConfig.config.getProperty("stock.hbase.table.name")));
    }
}
