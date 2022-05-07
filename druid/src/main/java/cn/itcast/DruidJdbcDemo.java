package cn.itcast;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DruidJdbcDemo {
    /**
     * 使用JDBC连接Apache Druid数据库
     */

    public static void main(String[] args) throws Exception {

        /**
         * 1.加载Druid JDBC驱动
         * 2.获取Druid JDBC连接
         * 3.构建SQL语句
         * 4.构建Statement，执行SQL获取结果集
         * 5关闭Druid连接
         */
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        //2.获取Druid JDBC连接
        Connection conn = DriverManager.getConnection("jdbc:avatica:remote:url=http://node3:8888/druid/v2/sql/avatica/ ");
        //3.构建SQL语句
        String sql = "SELECT * FROM \"metrics-kafka\"";
        //4.构建Statement，执行SQL获取结果集
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()){

            String time = rs.getString(1);
            String url = rs.getString("url");
            System.out.println("time:"+time +",url:"+url);
        }

        //5关闭Druid连接
        if(rs != null){
            rs.close();
        }
        if(st != null){
            st.close();
        }
        if(conn != null){
            conn.close();
        }
    }

}
