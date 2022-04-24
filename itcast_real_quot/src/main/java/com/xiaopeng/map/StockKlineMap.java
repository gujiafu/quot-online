package com.xiaopeng.map;

import com.xiaopeng.bean.StockBean;
import com.xiaopeng.constant.Constant;
import com.xiaopeng.util.DateUtil;
import com.xiaopeng.util.DbUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Map;

/**
 * @Date 2021
 * 个股K线数据转换（日，周，月）
 */
public class StockKlineMap extends RichMapFunction<StockBean, Row> {
    /**
     * 一、初始化
     * 1.创建构造方法
     * 入参：kType：K线类型
     * firstTxdate：周期首个交易日
     * 2.获取交易日历表交易日数据
     * 3.获取周期首个交易日和T日
     * 4.获取K(周、月)线下的汇总表数据（高、低、成交量、金额）
     */
    // 1.创建构造方法
    String kType;//日K：1，周K：2，月K：3
    String firstTxdate;

    Map<String, Map<String, Object>> klineMap = null;
    String tradeDate = null;
    String firstTradeDate = null;

    public StockKlineMap(String kType, String firstTxdate) {
        this.kType = kType;
        this.firstTxdate = firstTxdate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //2.获取交易日历表交易日数据
        String sql = "SELECT * FROM tcc_date WHERE trade_date = CURDATE()";
        Map<String, String> mapData = DbUtil.queryKv(sql);
        // 3.获取周期首个交易日和T日
        //如果是日K,周期的首个交易日,和当天交易日是同一个日期
        //如果不是日K,那么周期的首个交易日就有可能小于天交易日
        tradeDate = mapData.get("trade_date");//当天交易日
        firstTradeDate = mapData.get(firstTxdate); //周期的首个交易日

        //4.获取K(周、月)线下的汇总表数据（高、低、成交量、金额）
        String sql2 = "SELECT \n" +
                "sec_code ,\n" +
                "MAX(high_price) as high_price,\n" +
                "MIN(low_price) as low_price,\n" +
                "SUM(trade_vol) as trade_vol,\n" +
                "SUM(trade_amt) as trade_amt \n" +
                "FROM bdp_quot_stock_kline_day\n" +
                "WHERE trade_date BETWEEN " + firstTradeDate + " AND " + tradeDate + " \n" +
                "GROUP BY 1";

        klineMap = DbUtil.query("sec_code", sql2);

    }

    /**
     * 二、业务处理
     * 1.获取个股部分数据（前收、收、开盘、高、低、量、金额）
     * 2.获取T日和周首次交易日时间,转换成long型
     * 3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
     * 4.获取周/月K数据：成交量、成交额、高、低
     * 5.高、低价格比较
     * 6.计算成交量、成交额
     * 7.计算均价
     * 8.封装数据Row
     */
    @Override
    public Row map(StockBean value) throws Exception {

        //1.获取个股部分数据（前收、收、开盘、高、低、量、金额）
        BigDecimal preClosePrice = value.getPreClosePrice();
        BigDecimal openPrice = value.getOpenPrice();
        BigDecimal closePrice = value.getClosePrice();
        BigDecimal highPrice = value.getHighPrice();
        BigDecimal lowPrice = value.getLowPrice();
        Long tradeAmtDay = value.getTradeAmtDay();
        Long tradeVolDay = value.getTradeVolDay();

        //2.获取T日和周首次交易日时间,转换成long型
        Long tradeTime = DateUtil.stringToLong(tradeDate, Constant.format_yyyymmdd);
        Long firstTradeTime = DateUtil.stringToLong(firstTradeDate, Constant.format_yyyymmdd);
        //3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
        if (firstTradeTime < tradeTime && (kType.equals("2") || kType.equals("3"))) { //一定是周K/月K

            Map<String, Object> dataKline = klineMap.get(value.getSecCode());
            if(dataKline != null){
                   //4.获取周/月K数据：成交量、成交额、高、低
                BigDecimal highPriceLast = new BigDecimal(dataKline.get("high_price").toString());
                BigDecimal lowPriceLast = new BigDecimal(dataKline.get("low_price").toString());
                Long tradeVolLast = Long.valueOf(dataKline.get("trade_vol").toString());
                Long tradeAmtLast = Long.valueOf(dataKline.get("trade_amt").toString());

                //5.高、低价格比较
                if(highPrice.compareTo(highPriceLast) == -1){
                    highPrice = highPriceLast;
                }
                if(lowPrice.compareTo(lowPriceLast) == 1){
                    lowPrice = lowPriceLast;
                }
                //6.计算成交量、成交额
                tradeVolDay += tradeVolLast;
                tradeAmtDay += tradeAmtLast;
            }
        }
        // 7.计算均价
        BigDecimal avgPrice = new BigDecimal(0);
        if(tradeVolDay != 0){
            avgPrice = new BigDecimal(tradeAmtDay).divide(new BigDecimal(tradeVolDay),2,RoundingMode.HALF_UP);
        }

        //8.封装数据Row
        //日K不需要计算
        Row row = new Row(13);
        row.setField(0,new Timestamp(value.getEventTime()));
        row.setField(1,tradeDate);
        row.setField(2,value.getSecCode());
        row.setField(3,value.getSecName());
        row.setField(4,kType);
        row.setField(5,preClosePrice);
        row.setField(6,openPrice);
        row.setField(7,highPrice);
        row.setField(8,lowPrice);
        row.setField(9,closePrice);
        row.setField(10,avgPrice);
        row.setField(11,tradeVolDay);
        row.setField(12,tradeAmtDay);
        return row;
    }
}
