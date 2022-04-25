package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectFunction;
import cn.itcast.function.StockMinWindowFunction;
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
 * 2.个股分时行情
 */
public class StockQuotMinTask implements ProcessDataInterface {
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
        //1.定义侧边流,封装深市行情数据
        OutputTag<StockBean> szseOpt = new OutputTag<>("szseOpt", TypeInformation.of(StockBean.class));
        //2.数据分组
        SingleOutputStreamOperator<StockBean> result = waterData.keyBy(new KeySelectFunction())
                // 3.划分时间窗口
                .timeWindow(Time.minutes(1))
                //4.分时数据处理（新建分时窗口函数）
                .apply(new StockMinWindowFunction()) //获取的是分时行情数据
                //5.数据分流
                .process(new ProcessFunction<StockBean, StockBean>() {
                    @Override
                    public void processElement(StockBean value, Context ctx, Collector<StockBean> out) throws Exception {
                        if (value.getSource().equals("sse")) {
                            out.collect(value);
                        } else {
                            ctx.output(szseOpt, value);//封装深市行情数据
                        }
                    }
                });

        // 6.数据分流转换
        //只包含了沪市数据
        SingleOutputStreamOperator<String> sseStr = result.map(new MapFunction<StockBean, String>() {
            @Override
            public String map(StockBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        //获取深市数据,并转换
        SingleOutputStreamOperator<String> szseStr = result.getSideOutput(szseOpt).map(new MapFunction<StockBean, String>() {
            @Override
            public String map(StockBean value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        //7.分表存储(写入kafka)
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        //创建kafka生产者对象
        FlinkKafkaProducer011<String> sseKafka = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.stock.topic"), new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> szseKafka = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("szse.stock.topic"), new SimpleStringSchema(), properties);

        //执行写入操作
        sseStr.addSink(sseKafka);
        szseStr.addSink(szseKafka);
    }
}
