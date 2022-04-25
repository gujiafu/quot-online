package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockIncrBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class StockMinIncrWindowFunction extends RichWindowFunction<CleanBean, StockIncrBean, String, TimeWindow> {
    /**
     * 开发步骤：
     * 1.新建StockMinIncrWindowFunction 窗口函数
     * 2.记录最新个股
     * 3.格式化日期
     * 4.指标计算
     *   涨跌、涨跌幅、振幅
     * 5.封装输出数据
     */
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockIncrBean> out) throws Exception {


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
        //3.格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(line.getEventTime(), Constant.format_YYYYMMDDHHMMSS);

        //指标计算
        //涨跌、涨跌幅、振幅
        //今日涨跌=当前价-前收盘价
        BigDecimal upDown = line.getTradePrice().subtract(line.getPreClosePrice());
        //今日涨跌幅（%）=（当前价-前收盘价）/ 前收盘价 * 100%
        //保留两位小数,四舍五入
        BigDecimal increase = upDown.divide(line.getPreClosePrice(), 2, RoundingMode.HALF_UP);
        //今日振幅 =（当日最高点的价格－当日最低点的价格）/昨天收盘价×100%
        BigDecimal amplitude = (line.getMaxPrice().subtract(line.getMinPrice())).divide(line.getPreClosePrice(), 2, BigDecimal.ROUND_HALF_UP);

        //5.封装输出数据

        out.collect(new StockIncrBean(
                line.getEventTime(),
                line.getSecCode(),
                line.getSecName(),
                increase,
                line.getTradePrice(),
                upDown,
                line.getTradeVolumn(),
                amplitude,
                line.getPreClosePrice(),
                line.getTradeAmt(),
                tradeTime,
                line.getSource()
        ));
    }

}
