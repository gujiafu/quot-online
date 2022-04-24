package com.xiaopeng.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 数据清洗bean对象，接收avro对象数据
 * 包含:个股,指数,债券,基金
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CleanBean {

    private String mdStreamId;
    private String secCode;
    private String secName;
    private Long tradeVolumn; //日成就总量
    private Long tradeAmt; //日成就总金额
    private BigDecimal preClosePrice;
    private BigDecimal openPrice;
    private BigDecimal maxPrice; //最高价
    private BigDecimal minPrice;//最低价
    private BigDecimal tradePrice;
    private Long eventTime;
    private String source; //区分数据来源 sse||szse
}
