package cn.itcast.job;

import cn.itcast.avro.AvroDeserializerSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.map.SseMap;
import cn.itcast.map.SzseMap;
import cn.itcast.config.QuotConfig;
import cn.itcast.task.IndexQuotMinHdfsTask;
import cn.itcast.task.IndexQuotMinTask;
import cn.itcast.task.IndexQuotSecTask;
import cn.itcast.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Date 2021
 * 指数业务:秒级,分时,分时数据备份
 */
public class IndexStream {

    public static void main(String[] args) throws Exception {
        /**
         * 个股总体开发步骤：
         *  1.创建IndexStream单例对象，创建main方法
         *  2.获取流处理执行环境
         *  3.设置事件时间、并行度
         *  4.设置检查点机制
         *  5.设置重启机制
         *  6.整合Kafka(新建反序列化类)
         *  7.数据过滤（时间和null字段）
         *  8.数据转换、合并
         *  9.过滤个股数据
         *  10.设置水位线
         *  11.业务数据处理
         *  12.触发执行
         */
            //2.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.设置事件时间、并行度
        env.setParallelism(1);//开发环境便于测试，你设置1个，生产环境与kafka的分区数保持一致
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //4.设置检查点机制
        env.enableCheckpointing(5000l);//发送检查点的时间间隔
        env.setStateBackend(new FsStateBackend("hdfs://node01:8020/checkpoint/index"));//状态后端，保存检查点的路径
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//强一致性，保证数据只会消费一次
        env.getCheckpointConfig().setCheckpointTimeout(60000l);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//当检查点制作失败的时候，任务继续运行
        //当任务取消的时候，保留检查点，缺点是：需要手动删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //5.设置重启机制
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        //6.整合Kafka(新建反序列化类)
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", QuotConfig.config.getProperty("group.id"));

        //消费kafka数据
        //消费sse
        FlinkKafkaConsumer011<SseAvro> sseKafkaConsumer = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.config.getProperty("sse.topic"), new AvroDeserializerSchema(QuotConfig.config.getProperty("sse.topic")), properties);
        //消费szse
        FlinkKafkaConsumer011<SzseAvro> szseKafkaConsumer = new FlinkKafkaConsumer011<SzseAvro>(QuotConfig.config.getProperty("szse.topic"), new AvroDeserializerSchema(QuotConfig.config.getProperty("szse.topic")), properties);

        sseKafkaConsumer.setStartFromEarliest();
        szseKafkaConsumer.setStartFromEarliest();

        //沪市
        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafkaConsumer);
        //深市
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseKafkaConsumer);

        //7.数据过滤（时间和null字段）
        //null字段，在我们这里就是数据为0的字段
        //沪市过滤
        SingleOutputStreamOperator<SseAvro> sseFilterData = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro value) throws Exception {
                return QuotUtil.checkData(value) && QuotUtil.checkTime(value);
            }
        });
        //深市过滤
        SingleOutputStreamOperator<SzseAvro> szseFilterData = szseSource.filter(new FilterFunction<SzseAvro>() {
            @Override
            public boolean filter(SzseAvro value) throws Exception {
                return QuotUtil.checkData(value) && QuotUtil.checkTime(value);
            }
        });

        //8.数据转换、合并
        DataStream<CleanBean> unionData = sseFilterData.map(new SseMap()).union(szseFilterData.map(new SzseMap()));

        //9.过滤指数数据
        SingleOutputStreamOperator<CleanBean> stockData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean value) throws Exception {
                return QuotUtil.isIndex(value);
            }
        });

        //10.设置水位线
        DataStream<CleanBean> waterData = stockData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CleanBean>(Time.seconds(Long.valueOf(QuotConfig.config.getProperty("delay.time")))) {
            @Override
            public long extractTimestamp(CleanBean element) {
                return element.getEventTime();
            }
        });

        waterData.print("<<<指数<<:");

        /**
         * 1.秒级行情
         * 2.分时行情
         * 3.分时数据备份
         */
        //1.秒级行情
        new IndexQuotSecTask().process(waterData);
        //2.分时行情
        new IndexQuotMinTask().process(waterData);
        //3.分时数据备份
        new IndexQuotMinHdfsTask().process(waterData);

        // 12.触发执行
        env.execute("index stream");
    }
}
