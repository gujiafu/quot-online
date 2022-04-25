package cn.itcast.map;

import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.Timestamp;

/**
 * 个股分时数据写入hdfs的转换操作,目的是获取字符串,为建表和插入数据方便
 */
public class StockSinkHdfsMap extends RichMapFunction<StockBean,String> {

    /**
     * 开发步骤:
     * 1.定义字符串字段分隔符
     * 2.日期转换和截取：date类型
     * 3.新建字符串缓存对象
     * 4.封装字符串数据
     *
     * 字符串拼装字段顺序：
     * Timestamp|date|secCode|secName|preClosePrice|openPirce|highPrice|
     * lowPrice|closePrice|tradeVol|tradeAmt|tradeVolDay|tradeAmtDay|source
     */
    String sp = QuotConfig.config.getProperty("hdfs.seperator");
    @Override
    public String map(StockBean value) throws Exception {
        //2.日期转换和截取：date类型(yyyy-MM-dd)
        String tradeDate = DateUtil.longTimeToString(value.getEventTime(), Constant.format_yyyy_mm_dd);
        //3.新建字符串缓存对象
        StringBuilder sb = new StringBuilder();
        sb.append(new Timestamp(value.getEventTime())).append(sp)
               .append(tradeDate).append(sp)
               .append(value.getSecCode()).append(sp)
               .append(value.getSecName()).append(sp)
               .append(value.getPreClosePrice()).append(sp)
                .append(value.getOpenPrice()).append(sp)
                .append(value.getHighPrice()).append(sp)
                .append(value.getLowPrice()).append(sp)
                .append(value.getClosePrice()).append(sp)
                .append(value.getTradeVol()).append(sp)
                .append(value.getTradeAmt()).append(sp)
                .append(value.getTradeVolDay()).append(sp)
                .append(value.getTradeAmtDay()).append(sp)
                .append(value.getSource());

        return sb.toString();
    }
}
