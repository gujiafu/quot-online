package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 个股对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockBean {

    private Long eventTime; //事件时间
    private String secCode;
    private String secName;
    private BigDecimal preClosePrice;
    private BigDecimal openPrice;
    private BigDecimal highPrice; //最高价
    private BigDecimal lowPrice; //最低价
    private BigDecimal closePrice;
    private Long tradeVol;//分时成交量
    private Long tradeAmt;//分时成交金额
    private Long tradeVolDay;//日成交总量量
    private Long tradeAmtDay;//日成交总金额
    private Long tradeTime; //格式化时间
    private String source;


}
