package com.xiaopeng.task;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.bean.WarnAmplitudeBean;
import com.xiaopeng.bean.WarnBaseBean;
import com.xiaopeng.inter.ProcessDataCepInterface;
import com.xiaopeng.mail.MailSend;
import com.xiaopeng.util.RedisUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @Date 2021
 * 振幅
 */
public class AmplitudeTask implements ProcessDataCepInterface {
    @Override
    public void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env) {
        //计算公式：振幅： (max(最高点位)-min(最低点位))/期初前收盘点位*100% ,secCode,eventTime
        /**
         * 1.数据转换
         * 2.初始化表执行环境
         * 3.注册表（流）
         * 4.sql执行
         * 5.表转流
         * 6.模式匹配,比较阀值
         * 7.查询数据
         * 8.发送告警邮件
         */
        //1.数据转换
        SingleOutputStreamOperator<WarnBaseBean> mapData = waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
            @Override
            public WarnBaseBean map(CleanBean value) throws Exception {

                return new WarnBaseBean(
                        value.getSecCode(),
                        value.getPreClosePrice(),
                        value.getMaxPrice(),
                        value.getMinPrice(),
                        value.getTradePrice(),
                        value.getEventTime()
                );
            }
        });

        //3.注册表（流）
        //获取表
        StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);
        //注册表字段
        tblEnv.registerDataStream("tbl",mapData,"secCode,preClosePrice,highPrice,lowPrice,eventTime.rowtime");

        //4.sql执行
        String sql = " select secCode,preClosePrice,Round((max(highPrice)- min(lowPrice))/preClosePrice,2) as amplitude from tbl group by secCode,preClosePrice , tumble(eventTime, interval '2' second) ";
        Table table = tblEnv.sqlQuery(sql);

        //5.表转流
        DataStream<WarnAmplitudeBean> streamData = tblEnv.toAppendStream(table, WarnAmplitudeBean.class);

        //6.模式匹配,比较阀值
        //先查询redis中的振幅阈值
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        BigDecimal amplitudeThreshold = new BigDecimal(jedisCluster.hget("quot", "amplitude"));

        //定义模式匹配的规则
        Pattern<WarnAmplitudeBean, WarnAmplitudeBean> pattern = Pattern.<WarnAmplitudeBean>begin("begin").where(new SimpleCondition<WarnAmplitudeBean>() {
            @Override
            public boolean filter(WarnAmplitudeBean value) throws Exception {
                return value.getAmplitude().compareTo(amplitudeThreshold) == 1;
            }
        });
        //把模式规则作用在数据流上
        PatternStream<WarnAmplitudeBean> cep = CEP.pattern(streamData.keyBy(WarnAmplitudeBean::getSecCode), pattern);

        //7.查询数据
        cep.select(new PatternSelectFunction<WarnAmplitudeBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnAmplitudeBean>> pattern) throws Exception {

                List<WarnAmplitudeBean> begin = pattern.get("begin");
                //8.发送告警邮件
                if(begin != null && begin.size()>0){
                    MailSend.send("个股振幅告警:"+begin.toString());
                }
                return begin;
            }
        });
    }
}
