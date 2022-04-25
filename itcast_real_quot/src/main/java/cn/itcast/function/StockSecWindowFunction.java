package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 个股秒级行情业务处理
 */
// 1.新建StockSecWindowFunction 窗口函数
public class StockSecWindowFunction extends RichWindowFunction<CleanBean, StockBean, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockBean> out) throws Exception {
        /**
         * 开发步骤：
         * 1.新建StockSecWindowFunction 窗口函数
         * 2.记录最新个股
         * 3.格式化日期
         * 4.封装输出数据
         */

        //2.记录最新个股
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
        Long tradeTime = DateUtil.longTimeTransfer(line.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        //4.封装输出数据
        out.collect(new StockBean(
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
        ));

    }
}
