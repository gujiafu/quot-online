package cn.itcast.util;

import cn.itcast.constant.Constant;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtil {

    /**
     * String类型日期转long型时间
     * @param time
     * @param format
     * @return
     */
    public static Long  stringToLong(String time ,String format){

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    /**
     * Long型日期转Long型时间
     * @param time
     * @param format
     * @return
     */
    public static Long  longTimeTransfer(Long time ,String format){

        FastDateFormat dateFormat = FastDateFormat.getInstance(format);
        String str = dateFormat.format(new Date(time));
        Long lTime = Long.valueOf(str);
        return lTime;
    }

    /**
     * Long型日期转String型时间
     * @param time
     * @param format
     * @
     */
    public static String longTimeToString(Long time ,String format){

        FastDateFormat dateFormat = FastDateFormat.getInstance(format);
        String str = dateFormat.format(time);
        return str;
    }
    public static void main(String[] args) {

        Long time = 1596763038601L;
        String s = longTimeToString(time, Constant.format_yyyy_mm_dd);
        System.out.println("格式化日期："+s);
    }

}
