package com.xiaopeng.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 板块行情Bean对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SectorBean {

    private Long eventTime;
    private String sectorCode;
    private String sectorName;
    private BigDecimal preClosePrice;
    private BigDecimal openPrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closePrice;
    private Long tradeVol;
    private Long tradeAmt;
    private Long tradeVolDay;
    private Long tradeAmtDay;
    private Long tradeTime;
}