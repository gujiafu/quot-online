package cn.itcast.inter;

import cn.itcast.bean.CleanBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 通用接口,解耦合
 * 预警业务使用（振幅）
 */
public interface ProcessDataCepInterface {

    void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env);
}
