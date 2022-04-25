package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.TurnoverRateBean;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.mail.MailSend;
import cn.itcast.util.DbUtil;
import cn.itcast.util.RedisUtil;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * @Date 2021
 * 换手率
 */
public class TurnoverRateTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 1.创建bean对象
         * 2.数据转换process
         *   (1)加载mysql流通股本数据
         *   (2)封装bean对象数据
         * 3.加载redis换手率数据
         * 4.模式匹配
         * 5.查询数据
         * 6.发送告警邮件
         * 7.数据打印
         */
        //换手率 = 成交量/流通股本×100%
        //2.数据转换process
        //  (1)加载mysql流通股本数据
        //  (2)封装bean对象数据
        SingleOutputStreamOperator<TurnoverRateBean> prcessData = waterData.process(new ProcessFunction<CleanBean, TurnoverRateBean>() {
            Map<String, Map<String, Object>> map = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //加载mysql数据,查询个股流通股本
                String sql = "SELECT * FROM bdp_sector_stock";
                map = DbUtil.query("sec_code", sql);
            }

            @Override
            public void processElement(CleanBean value, Context ctx, Collector<TurnoverRateBean> out) throws Exception {

                Map<String, Object> stockMap = map.get(value.getSecCode());
                if (stockMap != null) {
                    BigDecimal negoCap = new BigDecimal(stockMap.get("nego_cap").toString());
                    out.collect(new TurnoverRateBean(
                            value.getSecCode(),
                            value.getSecName(),
                            value.getTradePrice(),
                            value.getTradeVolumn(),
                            negoCap
                    ));
                }
            }
        });

        //3.加载redis换手率数据
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        BigDecimal turnoverRateThreshold = new BigDecimal(jedisCluster.hget("quot", "turnoverRate"));

        //4.模式匹配
        Pattern<TurnoverRateBean, TurnoverRateBean> pattern = Pattern.<TurnoverRateBean>begin("begin")
                .where(new SimpleCondition<TurnoverRateBean>() {
                    @Override
                    public boolean filter(TurnoverRateBean value) throws Exception {
                        BigDecimal turnoverRate = new BigDecimal(value.getTradeVol()).divide(value.getNegoCap(), 2, RoundingMode.HALF_UP);
                        return turnoverRate.compareTo(turnoverRateThreshold) == 1;
                    }
                });
        //5.查询数据
        //获取模式匹配数据流
        PatternStream<TurnoverRateBean> cep = CEP.pattern(prcessData.keyBy(TurnoverRateBean::getSecCode), pattern);

        cep.select(new PatternSelectFunction<TurnoverRateBean, Object>() {
            @Override
            public Object select(Map<String, List<TurnoverRateBean>> pattern) throws Exception {
                List<TurnoverRateBean> begin = pattern.get("begin");
                if(begin != null && begin.size()>0){
                    // 6.发送告警邮件
                    MailSend.send("换手率实时预警:"+begin.toString());
                }
                return begin;
            }
        }).print("换手率实时预警:");//7.数据打印


    }
}
