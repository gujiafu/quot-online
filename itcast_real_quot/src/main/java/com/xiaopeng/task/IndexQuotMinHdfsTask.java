package com.xiaopeng.task;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.config.QuotConfig;
import com.xiaopeng.function.IndexMinWindowFunction;
import com.xiaopeng.inter.ProcessDataInterface;
import com.xiaopeng.map.IndexSinkHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * @Date 2021
 * 指数分时数据备份
 */
public class IndexQuotMinHdfsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

        /**
         * 1.设置HDFS存储路径
         * 2.设置数据文件参数
         *   （大小、分区、格式、前缀、后缀）
         * 3.数据分组
         * 4.划分时间窗口
         * 5.数据处理
         * 6.转换并封装数据
         * 7.写入HDFS
         */
        //1.设置HDFS存储路径
        //BucketingSink 此对象是实时写入文件的对象
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("index.sec.hdfs.path"));
        //2.设置数据文件参数
        //  （大小、分区、格式、前缀、后缀）
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.config.getProperty("hdfs.bucketer"))); //分区 yyyyMMdd
//        bucketingSink.setBucketer(new DateTimeBucketer<>("yyyy/MM/dd")); //分区
        bucketingSink.setWriter(new StringWriter<>());//写入字符串数据
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch"))); //文件大小

        bucketingSink.setInProgressPrefix("index-"); //表示正在写入的文件
        bucketingSink.setPendingPrefix("index2-"); //文件写入完成之后变成pending状态
        bucketingSink.setPendingSuffix(".txt");
        bucketingSink.setInProgressSuffix(".txt");

        //3.数据分组
        waterData.keyBy(CleanBean::getSecCode)
                .timeWindow(Time.minutes(1))
                .apply(new IndexMinWindowFunction())
                .map(new IndexSinkHdfsMap())
                .addSink(bucketingSink);
    }
}
