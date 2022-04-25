package cn.itcast.util;

import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;

/**
 * 数据过滤工具类
 */
public class QuotUtil {

    /**
     * 时间过滤:只过滤时间范围:9:30--15:00
     */
    public static Boolean checkTime(Object obj) {
        if (obj instanceof SseAvro) {
            SseAvro sseAvro = (SseAvro) obj;
            return sseAvro.getTimestamp() < SpecialTimeUtil.closeTime && sseAvro.getTimestamp() > SpecialTimeUtil.openTime;
        } else {
            SzseAvro szseAvro = (SzseAvro) obj;
            return szseAvro.getTimestamp() < SpecialTimeUtil.closeTime && szseAvro.getTimestamp() > SpecialTimeUtil.openTime;
        }
    }

    /**
     * 数据过滤(高开低收都不能为0)
     */
    public static Boolean checkData(Object obj) {
        if (obj instanceof SseAvro) {
            SseAvro sseAvro = (SseAvro) obj;
            return sseAvro.getHighPrice() != 0 && sseAvro.getOpenPrice() != 0 && sseAvro.getLowPrice() != 0 && sseAvro.getClosePx() != 0;
        } else {
            SzseAvro szseAvro = (SzseAvro) obj;
            return szseAvro.getHighPrice() != 0 && szseAvro.getOpenPrice() != 0 && szseAvro.getLowPrice() != 0 && szseAvro.getClosePx() != 0;
        }
    }

    /**
     * 个股数据过滤
     */
    public static boolean isStock(CleanBean value) {
        return value.getMdStreamId().equals("MD002") || value.getMdStreamId().equals("010");
    }

    /**
     * 指数数据过滤
     */
    public static boolean isIndex(CleanBean value) {
        return value.getMdStreamId().equals("MD001") || value.getMdStreamId().equals("900");
    }
}
