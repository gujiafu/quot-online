package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 个股分时行情业务处理
 */
// 1.新建StockMinWindowFunction 窗口函数
public class StockMinWindowFunction extends RichWindowFunction<CleanBean, StockBean, String, TimeWindow> {
    //2.初始化 MapState<String, StockBean> ,数据是保存在内存里的,就是map数据结构
    //保存的是上一窗口的数据
    MapState<String, StockBean> stockMs = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        stockMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, StockBean>("stockMs", String.class, StockBean.class));
    }

    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockBean> out) throws Exception {
        /**
         * 开发步骤：
         * 1.新建StockMinWindowFunction 窗口函数
         * 2.初始化 MapState<String, StockBean>
         * 3.记录最新个股
         * 4.获取分时成交额和成交数量
         * 5.格式化日期
         * 6.封装输出数据
         * 7.更新MapState
         */
        // 3.记录最新个股
        CleanBean line= null; //最新一条数据
        //遍历输入数据集
        for (CleanBean cleanBean : input) {
            if(line == null){
                line = cleanBean;
            }
            //根据时间判断是否是最新一条数据
            if(line.getEventTime() <cleanBean.getEventTime()){
                line = cleanBean;
            }
        }

        //4.获取分时成交额和成交数量
        StockBean stockBeanLast = stockMs.get(line.getSecCode());
        //设置分时数据
        long minVol = 0l; //分时成交量
        long minAmt = 0l; //分时成交金额
        if(stockBeanLast != null){
            minVol = line.getTradeVolumn() - stockBeanLast.getTradeVolDay();
            minAmt = line.getTradeAmt() - stockBeanLast.getTradeAmtDay();
        }

        //5.格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(line.getEventTime(), Constant.format_YYYYMMDDHHMMSS);

        //6.封装输出数据
        StockBean stockBean = new StockBean(
                line.getEventTime(),
                line.getSecCode(),
                line.getSecName(),
                line.getPreClosePrice(),
                line.getOpenPrice(),
                line.getMaxPrice(),
                line.getMinPrice(),
                line.getTradePrice(),
                minVol, minAmt,
                line.getTradeVolumn(),
                line.getTradeAmt(),
                tradeTime,
                line.getSource()
        );
        out.collect(stockBean);

        //7.更新MapState
        stockMs.put(line.getSecCode(),stockBean);



    }
}
