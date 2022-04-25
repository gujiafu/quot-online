package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Date 2021
 * 换手率bean
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TurnoverRateBean {
    /**
     * 证券代码
     */
    private String secCode;
    /**
     * 证券名称
     */
    private String secName;
    /**
     * 当前价/收盘价
     */
    private BigDecimal tradePrice;
    /**
     * 成交量
     */
    private Long tradeVol;
    /**
     * 股本
     */
    private BigDecimal negoCap;
}
