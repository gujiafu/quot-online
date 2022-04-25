package cn.itcast.inter;

import cn.itcast.bean.CleanBean;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 通用接口,子类实现此接口,覆写process方法
 */
public interface ProcessDataInterface {
    void process(DataStream<CleanBean> waterData);
}
