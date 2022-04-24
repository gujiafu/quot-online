package com.xiaopeng.task;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.bean.WarnBaseBean;
import com.xiaopeng.inter.ProcessDataInterface;
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
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * @Date 2021
 * 实时预警——涨跌幅
 */
public class UpDownTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 1.数据转换map
         * 2.封装样例类数据
         * 3.加载redis涨跌幅数据
         * 4.模式匹配
         * 5.获取匹配模式流数据
         *   6.查询数据
         * 7.发送告警邮件
         */
        //1.数据转换map
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
        // 3.加载redis涨跌幅数据
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        BigDecimal upRate = new BigDecimal(jedisCluster.hget("quot", "upDown2"));//涨幅
        BigDecimal udRate = new BigDecimal(jedisCluster.hget("quot", "upDown1"));//跌幅

        // 4.模式匹配
        Pattern<WarnBaseBean, WarnBaseBean> pattern = Pattern.<WarnBaseBean>begin("begin").where(new SimpleCondition<WarnBaseBean>() {
            @Override
            public boolean filter(WarnBaseBean value) throws Exception {
                //计算涨跌幅数据
                //涨跌幅：(期末收盘点位-期初前收盘点位)/期初前收盘点位*100%
                BigDecimal upDownThreshold = (value.getClosePrice().subtract(value.getPreClosePrice())).divide(value.getPreClosePrice(), 2, RoundingMode.HALF_UP);

                if (upDownThreshold.compareTo(upRate) == -1 && upDownThreshold.compareTo(udRate) == 1) {
                    return false;//正常范围的涨跌幅数据返回false
                }
                return true;
            }
        });

        // 5.获取匹配模式流数据
        PatternStream<WarnBaseBean> cep = CEP.pattern(mapData.keyBy(WarnBaseBean::getSecCode), pattern);

        // 6.查询数据
        cep.select(new PatternSelectFunction<WarnBaseBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnBaseBean>> pattern) throws Exception {
                List<WarnBaseBean> begin = pattern.get("begin");
                if(!begin.isEmpty() && begin.size()>0){
                    //7.发送告警邮件
                    MailSend.send("涨跌幅告警::"+begin.toString());
                }
                return begin;
            }
        }).print();

    }
}
