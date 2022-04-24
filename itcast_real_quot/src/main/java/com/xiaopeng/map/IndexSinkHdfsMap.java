package com.xiaopeng.map;

import com.xiaopeng.bean.IndexBean;
import com.xiaopeng.config.QuotConfig;
import com.xiaopeng.constant.Constant;
import com.xiaopeng.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.Timestamp;

/**
 * @Date 2021
 * 将indexBean转成string
 */
public class IndexSinkHdfsMap implements MapFunction<IndexBean,String> {

    //1.定义字符串字段分隔符
    String sp = QuotConfig.config.getProperty("hdfs.seperator");

    @Override
    public String map(IndexBean value) throws Exception {
        //2.日期转换和截取：date类型
        String tradeDate = DateUtil.longTimeToString(value.getEventTime(), Constant.format_yyyy_mm_dd);

        //字符串拼装字段顺序：
        //Timestamp|date|indexCode|indexName|preClosePrice|openPirce|highPrice|
        //lowPrice|closePrice|tradeVol|tradeAmt|tradeVolDay|tradeAmtDay|source
        StringBuffer buffer = new StringBuffer();
        buffer.append(new Timestamp(value.getEventTime())).append(sp)
                .append(tradeDate).append(sp)
                .append(value.getIndexCode()).append(sp)
                .append(value.getIndexName()).append(sp)
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
        return buffer.toString();
    }
}
