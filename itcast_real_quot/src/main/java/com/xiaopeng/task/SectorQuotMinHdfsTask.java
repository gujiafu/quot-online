package com.xiaopeng.task;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.config.QuotConfig;
import com.xiaopeng.function.KeySelectFunction;
import com.xiaopeng.function.SectorWindowFunction;
import com.xiaopeng.function.StockMinWindowFunction;
import com.xiaopeng.inter.ProcessDataInterface;
import com.xiaopeng.map.SectorSinkHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * @Date 2021
 * 板块分时数据备份
 */
public class SectorQuotMinHdfsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.设置HDFS存储路径
         * 2.设置数据文件参数
         *  （大小、分区、格式、前缀、后缀）
         * 3.数据分组
         * 4.划分个股时间窗口
         * 5.个股窗口数据处理
         * 6.划分板块时间窗口
         * 7.板块窗口数据处理
         * 8.转换并封装数据
         * 9.写入HDFS
         */
        //1.设置HDFS存储路径
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("sector.sec.hdfs.path"));
        //2.设置数据文件参数
        //  （大小、分区、格式、前缀、后缀）
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch")));
        bucketingSink.setWriter(new StringWriter<>());//格式
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.config.getProperty("hdfs.bucketer"))); //配置的是格式化模板
        //前缀、后缀
        bucketingSink.setInProgressPrefix("sector-");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingPrefix("sector2-");
        bucketingSink.setPendingSuffix(".txt");

        //3.数据分组
        waterData.keyBy(new KeySelectFunction())
                .timeWindow(Time.minutes(1))
                .apply(new StockMinWindowFunction())
                .timeWindowAll(Time.minutes(1))
                .apply(new SectorWindowFunction())
                //8.转换并封装数据
                .map(new SectorSinkHdfsMap())
                .addSink(bucketingSink);
    }
}
