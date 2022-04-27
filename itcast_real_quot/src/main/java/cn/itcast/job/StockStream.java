package cn.itcast.job;

import cn.itcast.avro.AvroDeserializerSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.map.SseMap;
import cn.itcast.map.SzseMap;
import cn.itcast.config.QuotConfig;
import cn.itcast.task.*;
import cn.itcast.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 个股流处理业务
 * 子业务包含:秒级行情,分时行情,K线行情,分数数据备份,涨跌幅行情
 */
public class StockStream {

    /**
     * 个股总体开发步骤：
     * 1.创建StockStream单例对象，创建main方法
     * 2.获取流处理执行环境
     * 3.设置事件时间、并行度
     * 4.设置检查点机制
     * 5.设置重启机制
     * 6.整合Kafka(新建反序列化类)
     * 7.数据过滤（时间和null字段）
     * 8.数据转换、合并
     * 9.过滤个股数据
     * 10.设置水位线
     * 11.业务数据处理
     * 12.触发执行
     */
    // 1.创建StockStream单例对象，创建main方法
    public static void main(String[] args) throws Exception {
        //2.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.设置事件时间、并行度
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //4.设置检查点机制
//        env.enableCheckpointing(5000L);
////        env.setStateBackend(new FsStateBackend("hdfs://node01:8020/checkpoint/stock"));
//        env.setStateBackend(new FsStateBackend("file:///D:\\export\\servers\\tmp\\checkpoint"));
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);//超时时间
//        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//检查点制作失败,任务继续运行
//        //取消任务的时候,保留检查点,不会自动删除,生产环境就使用这种
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        //5.设置重启机制
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        // 6.整合Kafka(新建反序列化类)
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", QuotConfig.config.getProperty("group.id"));
        //会消费SSE,自定义反序列化
        FlinkKafkaConsumer011<SseAvro> sseKafkaConsumer = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.config.getProperty("sse.topic"), new AvroDeserializerSchema(QuotConfig.config.getProperty("sse.topic")), properties);

        //消费SZSE
        FlinkKafkaConsumer011<SzseAvro> szseKafkaConsumer = new FlinkKafkaConsumer011<SzseAvro>(QuotConfig.config.getProperty("szse.topic"), new AvroDeserializerSchema(QuotConfig.config.getProperty("szse.topic")), properties);

        //配置消费模式
        sseKafkaConsumer.setStartFromEarliest();
        szseKafkaConsumer.setStartFromEarliest();

        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafkaConsumer);
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseKafkaConsumer);
//        sseSource.print("sse:");
//        szseSource.print("szse:<<<<<");

        //7.数据过滤（时间和null字段(高开低收都不能为0)）
        //返回true的数据
        SingleOutputStreamOperator<SseAvro> sseFilterData = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro value) throws Exception {
                return QuotUtil.checkTime(value) && QuotUtil.checkData(value);
            }
        });
        SingleOutputStreamOperator<SzseAvro> szseFilterData = szseSource.filter(new FilterFunction<SzseAvro>() {
            @Override
            public boolean filter(SzseAvro value) throws Exception {
                return QuotUtil.checkTime(value) && QuotUtil.checkData(value);
            }
        });

        //8.数据转换、合并
        DataStream<CleanBean> unionData = sseFilterData.map(new SseMap()).union(szseFilterData.map(new SzseMap()));
//        unionData.print("合并数据:");
        //9.过滤个股数据
        SingleOutputStreamOperator<CleanBean> filterData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean value) throws Exception {
                return QuotUtil.isStock(value);
            }
        });

        //10.设置水位线
        //设置水印:解决网络乱序和延迟
        SingleOutputStreamOperator<CleanBean> waterData = filterData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CleanBean>(Time.seconds(Long.valueOf(QuotConfig.config.getProperty("delay.time")))) { //设置延迟时间
            @Override
            public long extractTimestamp(CleanBean element) {
                return element.getEventTime();
            }
        });

//        SingleOutputStreamOperator<CleanBean> waterData2 = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<CleanBean>() {
//
//            long delayTime = 2000l;
//            long currentTimestamp = 0;
//
//            @Nullable
//            @Override
//            public Watermark getCurrentWatermark() {
//                return new Watermark(currentTimestamp - delayTime);
//            }
//
//            @Override
//            public long extractTimestamp(CleanBean element, long previousElementTimestamp) {
//                Long eventTime = element.getEventTime();
//                currentTimestamp = Math.max(currentTimestamp, eventTime);//谁大取谁
//                return eventTime;
//            }
//        });

        waterData.print("水位线数据:");

        /**
         * 11.业务数据处理
         * 1).秒级行情(5s)
         * 2).分时行情（60s）
         * 3).分时行情备份
         * 4).个股涨幅榜
         * 5).个股K线(日、周、月)
         */
        //1).秒级行情(5s)
        new StockQuotSecTask().process(waterData);
        //2).分时行情（60s）
        new StockQuotMinTask().process(waterData);
//        //3).分时行情备份
        new StockQuotMinHdfsTask().process(waterData);
//        //4).个股涨幅榜
        new StockQuotIncrTask().process(waterData);
//        //5).个股K线(日、周、月)
        new StockKlineTask().process(waterData);


        //12.触发执行
        env.execute("stock stream");
    }

}
