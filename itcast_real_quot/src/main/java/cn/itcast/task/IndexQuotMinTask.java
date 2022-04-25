package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.IndexMinWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @Date 2021
 * 指数分时行情
 */
public class IndexQuotMinTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

        /**
         * 开发步骤：
         * 1.定义侧边流
         * 2.数据分组
         * 3.划分时间窗口
         * 4.分时数据处理（新建分时窗口函数）
         * 5.数据分流
         * 6.数据分流转换
         * 7.分表存储(写入kafka)
         */
        //1.定义侧边流
        OutputTag<IndexBean> szseOpt = new OutputTag<>("szseOpt", TypeInformation.of(IndexBean.class));
        //2.数据分组
        SingleOutputStreamOperator<IndexBean> processData = waterData.keyBy(CleanBean::getSecCode)
                //3.划分时间窗口
                .timeWindow(Time.minutes(1))
                // 4.分时数据处理（新建分时窗口函数）
                .apply(new IndexMinWindowFunction())
                //5.数据分流
                .process(new ProcessFunction<IndexBean, IndexBean>() {
                    @Override
                    public void processElement(IndexBean value, Context ctx, Collector<IndexBean> out) throws Exception {
                        if (value.getSource().equals("sse")) {
                            out.collect(value);
                        } else {
                            ctx.output(szseOpt, value);
                        }
                    }
                });

        //6.数据分流转换
        //sse 数据转换
        SingleOutputStreamOperator<String> sseStr = processData.map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });
        //szse数据转换
        SingleOutputStreamOperator<String> szseStr = processData.getSideOutput(szseOpt).map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        //7.分表存储(写入kafka)
        //先获取kafka生产者对象,通过自定义sink将数据写入kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        FlinkKafkaProducer011<String> sseKafkaPro = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.index.topic"), new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> szseKafkaPro = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("szse.index.topic"), new SimpleStringSchema(), properties);
        //通过自定义sink将数据写入kafka
        sseStr.addSink(sseKafkaPro);
        szseStr.addSink(szseKafkaPro);
    }
}
