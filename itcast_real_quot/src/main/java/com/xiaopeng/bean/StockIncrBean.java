package com.xiaopeng.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;


/**
 * 个股涨跌幅对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockIncrBean {

    private Long eventTime;
    private String secCode;
    private String secName;
    private BigDecimal increase ;//涨跌幅
    private BigDecimal tradePrice ;//最新价
    private BigDecimal updown; //涨跌
    private Long tradeVol;//总手/总成交量
    private BigDecimal amplitude;//振幅
    private BigDecimal preClosePrice;
    private Long tradeAmt;//总成交金额
    private Long tradeTime; //格式化时间
    private String source;

}
