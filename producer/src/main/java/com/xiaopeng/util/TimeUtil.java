package com.xiaopeng.util;

import java.util.Calendar;
import java.util.Date;

/**
 * 时间工具类
 */
public class TimeUtil {

    public static long startTime = 0l;//开市时间-9:30
    public static long endTime = 0l; //闭市时间-15:00

    static {
        Calendar instance = Calendar.getInstance();
        instance.setTime(new Date());

        instance.set(Calendar.HOUR_OF_DAY,5);
        instance.set(Calendar.MINUTE,30);
        instance.set(Calendar.SECOND,0);

        startTime = instance.getTime().getTime();
        instance.set(Calendar.HOUR_OF_DAY,23);
        instance.set(Calendar.MINUTE,59);

        endTime = instance.getTime().getTime();
    }

    public static void main(String[] args) {
        System.out.println(startTime);
        System.out.println(endTime);
    }
}
