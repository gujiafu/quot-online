package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectFunction;
import cn.itcast.function.StockMinWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.StockSinkHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * 3.个股分时行情保存HDFS
 */
public class StockQuotMinHdfsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
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
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("stock.sec.hdfs.path"));
        //2.设置数据文件参数
        //  （大小、分区、格式、前缀、后缀）
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch")));
        bucketingSink.setWriter(new StringWriter<>());//格式
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.config.getProperty("hdfs.bucketer"))); //配置的是格式化模板
        //前缀、后缀
        //InProgress表示正在写数据
        //Pending表示数据写完之后,进入挂起状态,由InProgress进入到Pending状态
        //当检查点制作完成之后, 由Pending进入完成状态finished
        //如果你关闭检查点,永远是InProgress状态,进入不了完成状态finished
        bucketingSink.setInProgressPrefix("stock-");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingPrefix("stock2-");
        bucketingSink.setPendingSuffix(".txt");

        //3.数据分组
        waterData.keyBy(new KeySelectFunction())
                //4.划分时间窗口
                .timeWindow(Time.minutes(1))
                // 5.数据处理
                .apply(new StockMinWindowFunction()) //获取的是分时行情数据
                //6.转换并封装数据
                .map(new StockSinkHdfsMap())
                .addSink(bucketingSink);

    }
}
