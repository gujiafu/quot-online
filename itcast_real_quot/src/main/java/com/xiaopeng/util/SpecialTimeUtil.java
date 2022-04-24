package com.xiaopeng.util;

import com.xiaopeng.config.QuotConfig;

import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 * @Date 2020/6/9
 */
public class SpecialTimeUtil {

    static long openTime = 0;
    static long closeTime = 0;

    static {
        /**
         * 获取开市时间、闭市时间
         * 和日期时间数据
         */
        Properties properties  = QuotConfig.config;

        String date = properties.getProperty("date");//当天日期
        String beginTime  = properties.getProperty("proc.begin.time");
        String endTime = properties.getProperty("proc.end.time");

        Calendar calendar = Calendar.getInstance();

        if (date == null) {
            calendar.setTime(new Date());
        } else {
            calendar.setTimeInMillis(DateUtil.stringToLong(date, "yyyyMMdd"));
        }

        //设置开市时间
        if (beginTime == null) {
            calendar.set(Calendar.HOUR_OF_DAY, 9);
            calendar.set(Calendar.MINUTE, 30);
        } else {
            String[] arr = beginTime.split(":");
            calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(arr[0]));
            calendar.set(Calendar.MINUTE, Integer.parseInt(arr[1]));
        }

        //设置秒
        calendar.set(Calendar.SECOND, 0);
        //设置毫秒
        calendar.set(Calendar.MILLISECOND, 0);

        //获取开市时间
         openTime = calendar.getTime().getTime();

        //设置闭市时间
        if (endTime == null) {
            calendar.set(Calendar.HOUR_OF_DAY, 15);
            calendar.set(Calendar.MINUTE, 0);
        } else {
            String[] arr = endTime.split(":");
            calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(arr[0]));
            calendar.set(Calendar.MINUTE, Integer.parseInt(arr[1]));
        }

        //获取闭市时间
         closeTime = calendar.getTime().getTime();
    }

    public static void main(String[] args) {
        System.out.println(openTime);
        System.out.println(closeTime);
    }

}
