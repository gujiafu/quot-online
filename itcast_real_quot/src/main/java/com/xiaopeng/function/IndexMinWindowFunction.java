package com.xiaopeng.function;

import com.xiaopeng.bean.CleanBean;
import com.xiaopeng.bean.IndexBean;
import com.xiaopeng.constant.Constant;
import com.xiaopeng.util.DateUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Date 2021
 */
//1.新建IndexMinWindowFunction 窗口函数
public class IndexMinWindowFunction extends RichWindowFunction<CleanBean, IndexBean,String, TimeWindow> {

    /**
     * 开发步骤：
     * 1.新建MinIndexWindowFunction 窗口函数
     * 2.初始化 MapState<String, IndexBean>
     * 3.记录最新指数
     * 4.获取分时成交额和成交数量
     * 5.格式化日期
     * 6.封装输出数据
     * 7.更新MapState
     */
    MapState<String, IndexBean> indexMs = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        indexMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, IndexBean>("indexMs", String.class, IndexBean.class));
    }


    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<IndexBean> out) throws Exception {
        //3.记录最新指数
        CleanBean line = null;
        for (CleanBean cleanBean : input) {
            if(line == null){
                line = cleanBean;
            }
            if(line.getEventTime() < cleanBean.getEventTime()){
                line = cleanBean;
            }
        }

        //4.获取分时成交额和成交数量
        Long minTradeVol = 0l;
        Long minTradeAmt = 0l;
        IndexBean indexBeanLast = indexMs.get(line.getSecCode());
        if(indexBeanLast != null){
            minTradeVol = line.getTradeVolumn() - indexBeanLast.getTradeVolDay();
            minTradeAmt = line.getTradeAmt() - indexBeanLast.getTradeAmtDay();
        }

        //5.格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(line.getEventTime(), Constant.format_YYYYMMDDHHMMSS);

        //6.封装输出数据
        IndexBean indexBean = new IndexBean(
                line.getEventTime(),
                line.getSecCode(),
                line.getSecName(),
                line.getPreClosePrice(),
                line.getOpenPrice(),
                line.getMaxPrice(),
                line.getMinPrice(),
                line.getTradePrice(),
                minTradeVol,minTradeAmt,
                line.getTradeVolumn(),
                line.getTradeAmt(),
                tradeTime,
                line.getSource()
        );

        //收集输出数据
        out.collect(indexBean);
        //7.更新MapState
        indexMs.put(indexBean.getIndexCode(),indexBean);
    }
}
