package com.xiaopeng.function;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.bean.IndexBean;
import com.xiaopeng.constant.Constant;
import com.xiaopeng.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Date 2021
 * 指数秒级窗口函数处理
 */
public class IndexSecWindowFunction extends RichWindowFunction<CleanBean, IndexBean,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<IndexBean> out) throws Exception {

        /**
         * 开发步骤:
         * 1.获取最新一条数据
         * 2.格式化时间,拼接rowkey
         * 3.封装数据
         */
        //1.获取最新一条数据
        CleanBean line = null;
        for (CleanBean cleanBean : input) {
            if(line == null){
                line = cleanBean;
            }
            if(line.getEventTime() < cleanBean.getEventTime()){
                line = cleanBean;
            }
        }

        // 2.格式化时间,拼接rowkey
        Long tradeTime = DateUtil.longTimeTransfer(line.getEventTime(), Constant.format_YYYYMMDDHHMMSS);

        //3.封装数据
        //eventTime、indexCode、indexName、preClosePrice、openPrice、highPrice、lowPrice、closePrice、
        //tradeVol、tradeAmt、tradeVolDay、tradeAmtDay、tradeTime、source
        IndexBean indexBean = new IndexBean(
                line.getEventTime(),
                line.getSecCode(),
                line.getSecName(),
                line.getPreClosePrice(),
                line.getOpenPrice(),
                line.getMaxPrice(),
                line.getMinPrice(),
                line.getTradePrice(),
                0l,0l,
                line.getTradeVolumn(),
                line.getTradeAmt(),
                tradeTime,
                line.getSource()
        );

        out.collect(indexBean);
    }
}
