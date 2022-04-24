package com.xiaopeng.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Date 2021
 * 接收flink sql计算好的振幅数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WarnAmplitudeBean {

    //secCode、preClosePrice、amplitude
    private String secCode;
    private BigDecimal preClosePrice ;
    private BigDecimal amplitude ;//振幅

}
