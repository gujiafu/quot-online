package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.KlineType;
import cn.itcast.function.KeySelectFunction;
import cn.itcast.function.StockMinWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.StockKlineMap;
import cn.itcast.sink.SinkMysql;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Date 2021 ,此代码是通用代码
 * 个股K线（日、周、月）
 */
public class StockKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.新建侧边流分支（周、月）
         * 2.数据分组
         * 3.划分时间窗口
         * 4.数据处理
         * 5.分流、封装侧边流数据
         * 6.编写插入sql
         * 7.（日、周、月）K线数据写入
         * 数据转换
         * 数据分组
         * 数据写入mysql
         */
        //1.新建侧边流分支（周、月）
        OutputTag<StockBean> weekOpt = new OutputTag<>("weekOpt", TypeInformation.of(StockBean.class));
        OutputTag<StockBean> monthOpt = new OutputTag<>("monthOpt", TypeInformation.of(StockBean.class));
        //2.数据分组
        SingleOutputStreamOperator<StockBean> processData = waterData.keyBy(new KeySelectFunction())
                .timeWindow(Time.minutes(1))
                .apply(new StockMinWindowFunction())
                .process(new ProcessFunction<StockBean, StockBean>() { // 5.分流、封装侧边流数据
                    @Override
                    public void processElement(StockBean value, Context ctx, Collector<StockBean> out) throws Exception {
                        ctx.output(weekOpt, value); //周K
                        ctx.output(monthOpt, value); //月K
                        out.collect(value); //日k
                    }
                });

        //6.编写插入sql
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //7.（日、周、月）K线数据写入
        //日K
        //数据转换
        processData.map(new StockKlineMap(KlineType.DAYK.getType(), KlineType.DAYK.getFirstTxDateType()))
                //数据分组
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString(); //根据证券代码进行分组
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.day.table")))); // 数据写入mysql
        //周K
        processData.getSideOutput(weekOpt).map(new StockKlineMap(KlineType.WEEKK.getType(), KlineType.WEEKK.getFirstTxDateType()))
                //数据分组
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString(); //根据证券代码进行分组
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.week.table")))); // 数据写入mysql

        //月K
        processData.getSideOutput(monthOpt).map(new StockKlineMap(KlineType.MONTHK.getType(), KlineType.MONTHK.getFirstTxDateType()))
                //数据分组
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(2).toString(); //根据证券代码进行分组
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.month.table")))); // 数据写入mysql
    }
}
