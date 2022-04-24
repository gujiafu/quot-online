package com.xiaopeng.task;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.bean.StockIncrBean;
import com.xiaopeng.config.QuotConfig;
import com.xiaopeng.function.KeySelectFunction;
import com.xiaopeng.function.StockMinIncrWindowFunction;
import com.xiaopeng.inter.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * 4.个股涨跌幅
 * 也是基于分时数据进行处理的,区别于分时的是,多了一些统计指标
 * 今日涨跌=当前价-前收盘价
 * 今日涨跌幅（%）=（当前价-前收盘价）/ 前收盘价 * 100%
 * 今日振幅 =（当日最高点的价格－当日最低点的价格）/昨天收盘价×100%
 */
public class StockQuotIncrTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.创建bean对象
         * 4.分时数据处理（新建分时窗口函数）
         * 5.数据转换成字符串
         * 6.数据存储(单表)
         */

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        //创建kafka生产者对象
        FlinkKafkaProducer011<String> kafkaPro = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("stock.increase.topic"), new SimpleStringSchema(), properties);

        // 1.数据分组
        waterData.keyBy(new KeySelectFunction())
                //2.划分时间窗口
                .timeWindow(Time.minutes(1))
                // 4.分时数据处理（新建分时窗口函数）
                .apply(new StockMinIncrWindowFunction())
                //5.数据转换成字符串
                .map(new MapFunction<StockIncrBean, String>() {
                    @Override
                    public String map(StockIncrBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                }).addSink(kafkaPro);
    }
}
