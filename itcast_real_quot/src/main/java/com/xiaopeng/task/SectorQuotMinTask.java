package com.xiaopeng.task;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.bean.SectorBean;
import com.xiaopeng.config.QuotConfig;
import com.xiaopeng.function.KeySelectFunction;
import com.xiaopeng.function.SectorWindowFunction;
import com.xiaopeng.function.StockMinWindowFunction;
import com.xiaopeng.inter.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @Date 2021
 * 板块分时行情
 */
public class SectorQuotMinTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分个股时间窗口
         * 3.个股分时数据处理
         * 4.划分板块时间窗口
         * 5.板块分时数据处理
         * 6.数据转换成字符串
         * 7.数据写入kafka
         */

        //获取kafka生产者对象
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        //创建kafka生产者对象
        FlinkKafkaProducer011<String> kafkaPro = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.sector.topic"), new SimpleStringSchema(), properties);

        waterData.keyBy(new KeySelectFunction())
                .timeWindow(Time.minutes(1))
                .apply(new StockMinWindowFunction())
                .timeWindowAll(Time.minutes(1))
                .apply(new SectorWindowFunction())
                .map(new MapFunction<SectorBean, String>() {
                    @Override
                    public String map(SectorBean value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                }).addSink(kafkaPro);

    }
}
