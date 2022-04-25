package cn.itcast.sink;

import cn.itcast.util.DbUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @Date 2021
 * 写入mysql数据（通用）
 */
public class SinkMysql extends RichSinkFunction<Row> {

    String sql;

    public SinkMysql(String sql) {
        this.sql = sql;
    }

    Connection conn = null;
    PreparedStatement ps = null;

    //获取连接
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DbUtil.getConnByJdbc();
        ps = conn.prepareStatement(sql);
    }

    //执行操作
    @Override
    public void invoke(Row value, Context context) throws Exception {

        for (int i = 0; i < value.getArity(); i++) {
            ps.setObject(i+1, value.getField(i));
        }
        //执行插入
        ps.executeUpdate();
    }

    //关闭连接
    @Override
    public void close() throws Exception {
        if(ps != null){
            ps.close();
        }
        if(conn != null){
            conn.close();
        }
    }
}
