package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import org.apache.flink.api.java.functions.KeySelector;

public class KeySelectFunction implements KeySelector<CleanBean,String> {
    @Override
    public String getKey(CleanBean value) throws Exception {
        return value.getSecCode(); //证券代码是唯一标识
    }
}
