package com.xiaopeng.function;

import com.xiaopeng.bean.SectorBean;
import com.xiaopeng.bean.StockBean;
import com.xiaopeng.util.DbUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Date 2021
 * 板块行情业务逻辑加工，秒级和分时行情共用此类
 */
public class SectorWindowFunction extends RichAllWindowFunction<StockBean, SectorBean, TimeWindow> {

    /**
     * 一：open初始化
     * 1. 初始化数据：板块对应关系、最近交易日日K（上一交易日）
     * 2. 定义状态MapState<String, SectorBean >:上一个窗口板块数据
     * 3. 初始化基准价（1000）
     */

    Map<String, List<Map<String, Object>>> sectorStockMap = null;
    Map<String, Map<String, Object>> sectorKlineMap = null;
    MapState<String, SectorBean> sectorMs = null;
    //3. 初始化基准价（1000）
    BigDecimal basePrice = new BigDecimal(1000);
    @Override
    public void open(Configuration parameters) throws Exception {
        //1. 初始化数据：板块对应关系、板块日K表的最近交易日日K（上一交易日）
        String sql = "SELECT * FROM bdp_sector_stock where sec_abbr = 'ss'";
         sectorStockMap = DbUtil.queryForGroup("sector_code", sql);

         //板块日K表的最近交易日日K（上一交易日）
        String sql2 = "SELECT * FROM bdp_quot_sector_kline_day WHERE trade_date = (SELECT last_trade_date FROM tcc_date WHERE trade_date=CURDATE())";
        sectorKlineMap = DbUtil.query("sector_code", sql2);
        //2. 定义状态MapState<String, SectorBean >:上一个窗口板块数据
        sectorMs = getRuntimeContext().getMapState(new MapStateDescriptor<String, SectorBean>("sectorMs", String.class, SectorBean.class));
    }

    @Override
    public void apply(TimeWindow window, Iterable<StockBean> values, Collector<SectorBean> out) throws Exception {
        /**
         * 开发步骤
         * 1.循环窗口内个股数据并缓存
         * 2.轮询板块对应关系表下的个股数据、并获取指定板块下的个股
         * 3.初始化全部数据
         * 4.轮询板块下的个股数据，并获取板块名称、个股代码、个股流通股本和前一日板块总流通市值
         * 5.计算：开盘价/收盘价累计流通市值、累计（交易量、交易金额）（注意：是个股累计值，获取缓存的个股数据）
         *    累计个股开盘流通市值 = SUM(板块下的个股开盘价*个股流通股本)
         *    累计个股收盘流通市值 = SUM(板块下的个股收盘价*个股流通股本)
         * 6.判断是否是首日上市，并计算板块开盘价和收盘价
         *    板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的总流通市值/当前板块前一交易日板块总流通市值
         *    板块当前价格 = 板块前收盘价*当前板块以收盘价计算的总流通市值/当前板块前一交易日板块总流通市值
         * 7.初始化板块高低价
         * 8.获取上一个窗口板块数据（高、低、成交量、成交金额）
         * 9.计算最高价和最低价（前后窗口比较）
         * 10.计算分时成交量和成交金额
         * 11.开盘价与高低价比较
         * 12.封装结果数据
         * 13.缓存当前板块数据
         */
        //1.循环窗口内个股数据并缓存
        Map<String,StockBean> map = new HashMap<>();
        for (StockBean value : values) {
            map.put(value.getSecCode(),value);
        }

        //2.轮询板块对应关系表下的个股数据、并获取指定板块下的个股
        //会计算每一个板块的行情数据,包含高开低手,成交量/金额等
        for (String sectorCode : sectorStockMap.keySet()) {

            //获取指定板块下的个股
            List<Map<String, Object>> stockList = sectorStockMap.get(sectorCode);

            //3.初始化全部数据
            Long eventTime=0l;
            String sectorName = null;
            BigDecimal preClosePrice = new BigDecimal(0);
            BigDecimal openPrice = new BigDecimal(0);
            BigDecimal highPrice = new BigDecimal(0);
            BigDecimal lowPrice = new BigDecimal(0);
            BigDecimal closePrice = new BigDecimal(0);
            Long tradeVol=0l; //分时成就量
            Long tradeAmt=0l;//分时成就金额
            Long tradeVolDay=0l;//日成交总量
            Long tradeAmtDay=0l;//日成交总金额
            Long tradeTime=0l;//格式化之后的时间,拼接rowkey

            //4.轮询板块下的个股数据，
            // 累计个股开盘流通市值
            BigDecimal openCap = new BigDecimal(0);
            //累计个股收盘流通市值
            BigDecimal closeCap = new BigDecimal(0);
            BigDecimal preSectorNegoCap = new BigDecimal(0);
            for (Map<String, Object> sectorStockMap : stockList) {
                //并获取板块名称、个股代码、个股流通股本和前一日板块总流通市值
                sectorName = sectorStockMap.get("sector_name").toString();
                //个股代码
                String secCode = sectorStockMap.get("sec_code").toString();
                //个股流通股本
                BigDecimal negoCap = new BigDecimal(sectorStockMap.get("nego_cap").toString());
                //前一日板块总流通市值
                 preSectorNegoCap = new BigDecimal(sectorStockMap.get("pre_sector_nego_cap").toString());

                //5.计算：开盘价/收盘价累计流通市值、累计（交易量、交易金额）（注意：是个股累计值，获取缓存的个股数据）
                //   累计个股开盘流通市值 = SUM(板块下的个股开盘价*个股流通股本)
                //   累计个股收盘流通市值 = SUM(板块下的个股收盘价*个股流通股本)
                StockBean stockBean = map.get(secCode);//获取缓存的个股数据
                if(stockBean != null){ //说明这只股票既存在于实时流中,又存在于mysql(板块个股对应关系表)中
                    eventTime = stockBean.getEventTime();
                    tradeTime = stockBean.getTradeTime();
                    openCap = openCap.add(stockBean.getOpenPrice().multiply(negoCap));
                    closeCap = closeCap.add(stockBean.getClosePrice().multiply(negoCap));
                    //累计（交易量、交易金额）
                    tradeVolDay+= stockBean.getTradeVolDay();
                    tradeAmtDay += stockBean.getTradeAmtDay();
                }
            }

            //6.判断是否是首日上市，并计算板块开盘价和收盘价
            if(sectorKlineMap.isEmpty() || sectorKlineMap.get(sectorCode).isEmpty() ){ //没有历史日K的交易数据,说明此板块以前还不存在,说明是首日上市
                preClosePrice = basePrice;
            }else{ //非首日
                Map<String, Object> mapLast = sectorKlineMap.get(sectorCode);
                if(mapLast != null){
                    preClosePrice = new BigDecimal(mapLast.get("close_price").toString());
                }
            }
            //   板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的总流通市值/当前板块前一交易日板块总流通市值
            openPrice = preClosePrice.multiply(openCap).divide(preSectorNegoCap,2,BigDecimal.ROUND_HALF_UP);
            //   板块当前价格 = 板块前收盘价*当前板块以收盘价计算的总流通市值/当前板块前一交易日板块总流通市值
            closePrice = preClosePrice.multiply(closeCap).divide(preSectorNegoCap,2,RoundingMode.HALF_UP);

            // 7.初始化板块高低价
            highPrice = closePrice;
            lowPrice = closePrice;

            //8.获取上一个窗口板块数据（高、低、成交量、成交金额）
            SectorBean sectorBeanLast = sectorMs.get(sectorCode);
            if(sectorBeanLast != null){
                BigDecimal highPriceLast = sectorBeanLast.getHighPrice();
                BigDecimal lowPriceLast = sectorBeanLast.getLowPrice();
                Long tradeAmtDayLast = sectorBeanLast.getTradeAmtDay();
                Long tradeVolDayLast = sectorBeanLast.getTradeVolDay();
                // 9.计算最高价和最低价（前后窗口比较）
                if(highPrice.compareTo(highPriceLast) == -1){
                    highPrice = highPriceLast;
                }
                if(lowPrice.compareTo(lowPriceLast) == 1){
                    lowPrice = lowPriceLast;
                }
                //10.计算分时成交量和成交金额
                tradeAmt = tradeAmtDay - tradeAmtDayLast;
                tradeVol = tradeVolDay - tradeVolDayLast;
            }

            // 11.开盘价与高低价比较
            if(openPrice.compareTo(highPrice) == 1){
                highPrice = openPrice;
            }
            if(openPrice.compareTo(lowPrice) == -1){
                lowPrice = openPrice;
            }

            //12.封装结果数据
            SectorBean sectorBean = new SectorBean(
                eventTime,sectorCode,sectorName,preClosePrice,openPrice,highPrice,lowPrice,closePrice,tradeVol,tradeAmt,tradeVolDay,
                    tradeAmtDay,tradeTime
            );
            out.collect(sectorBean);

            // 13.缓存当前板块数据
            sectorMs.put(sectorCode,sectorBean);
        }

    }
}
