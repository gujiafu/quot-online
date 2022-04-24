package com.xiaopeng.szse;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * 深市行情实时行情广播数据
 * 服务端
 */
public class SocketServer {
    private static final int PORT = 4444;

    //随机浮动成交价格系数
    private static Double[] price = new Double[]{0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, -0.1, -0.11, -0.12, -0.13, -0.14, -0.15, -0.16, -0.17, -0.18, -0.19, -0.2};

    //随机浮动成交量
    private static int[] volumn = new int[]{50, 80, 110, 140, 170, 200, 230, 260, 290, 320, 350, 380, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300};

    //缓存成交总量/总金额
    public static Map<String, Map<String, Long>> map = new HashMap<>();

    //字段分隔符
    public static final String sp = "|";

    public static void main(String[] args) {
        ServerSocket server ;
        Socket socket ;
        DataOutputStream out ;
        try {

            //创建Socket服务端
            server = new ServerSocket(PORT, 1000, InetAddress.getByName("localhost"));
            socket = server.accept();
            //创建输出流对象
            out = new DataOutputStream(socket.getOutputStream());
            while (true) {
                Thread.sleep(1000); //模拟数据，1s对方广播一次
                //4.解析行数据
                List<String> list = parseFile();

                for (String line : list) {
                    out.writeUTF(line);//输出数据
                }
                out.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 深市行情（股票和指数）
     */
    private static List<String> parseFile() {
        ArrayList<String> list = new ArrayList<>();

        /**
         * 开发步骤：
         * 1.读取本地源数据
         * 2.获取随机浮动成交量和价格
         * 3.字符串切割、计算最新价
         * 4.获取缓存的成交量/金额
         * 5.计算总成交量/金额
         * 6.缓存总成交量/金额
         * 7.获取最高价和最低价(和最新价比较)
         * 8.封装结果数据
         */
        try {
            //1.读取本地源数据
            //本地磁盘路径
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("E:\\export\\servers\\tmp\\socket\\szse.txt")),"UTF-8"));
            //服务器磁盘路径
//            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("//export//servers//tmp//socket//szse.txt")),"UTF-8"));

            String str ;
            while ((str = br.readLine()) != null) {

                //2.获取随机浮动成交量和价格
                Random random = new Random();
                int pIndex = random.nextInt(price.length);
                //随机浮动价格系数
                Double randomPrice = price[pIndex];
                int vIndex = random.nextInt(volumn.length);
                //随机浮动成交量
                int randomVolumn = volumn[vIndex];

                //3.字符串切割、计算最新价
                String[] arr = str.split("\\|");
                String code = arr[1].trim();//产品代码

                //精度计算：金额字段加减乘除都用bigdecimal
                BigDecimal tradePrice = new BigDecimal(arr[9].trim());
                //26.73 *(1+0.1)
                //保存两位小数，四舍五入
                //浮动最新价
                tradePrice = tradePrice.multiply(new BigDecimal(1 + randomPrice)).setScale(2, RoundingMode.HALF_UP);


                //解析文件获取的成交量
                Long tradeVol = Long.valueOf(arr[3].trim());
                //解析文件获取的成交金额
                long tradeAmt = Double.valueOf(arr[4].trim()).longValue();

                //4.获取缓存的成交量/金额
                Long totalVol = 0l;
                Long totalAmt =0l;
                Map<String, Long> amtVolMap = map.get(code);
                //5.计算总成交量/金额
                if(amtVolMap == null){
                    totalVol = tradeVol;
                    totalAmt = tradeAmt;
                    //6.缓存总成交量和总成交金额
                    HashMap<String, Long> mapCache = new HashMap<>();
                    mapCache.put("tradeAmt",totalAmt);
                    mapCache.put("tradeVol",totalVol);
                    map.put(code,mapCache);
                }else{

                    //从缓存里获取的
                    Long tradeAmtTmp = amtVolMap.get("tradeAmt");
                    Long tradeVolTmp = amtVolMap.get("tradeVol");
                    totalVol = tradeVolTmp +  randomVolumn; //总成交量

                    //总成交金额
                    //总成交金额 ！= tradePrice * tradeVol
                    //增量的成交金额 = 浮动成交量* 最新价
                    //总成交金额 = 增量的成交金额+ 当前总金额
                    BigDecimal tmpAmt = tradePrice.multiply(new BigDecimal(randomVolumn));//增量的成交金额
                    totalAmt = tradeAmtTmp + tmpAmt.longValue();

                    //缓存总成交量和总成交金额
                    HashMap<String, Long> mapCache = new HashMap<>();
                    mapCache.put("tradeAmt",totalAmt);
                    mapCache.put("tradeVol",totalVol);
                    map.put(code,mapCache);
                }


                //7.获取最高价和最低价(和最新价比较)
                //获取最高价
                BigDecimal highPrice = new BigDecimal(arr[7].trim());
                if (tradePrice.compareTo(highPrice) == 1) {
                    highPrice = tradePrice;
                }

                //获取最低价
                BigDecimal lowPrice = new BigDecimal(arr[8].trim());
                if (tradePrice.compareTo(lowPrice) == -1) {

                    lowPrice = tradePrice;
                }

                //8.封装结果数据
                StringBuilder builder = new StringBuilder();
                builder.append(arr[0].trim()).append(sp)
                        .append(code).append(sp)
                        .append(arr[2].trim()).append(sp)
                        .append(totalVol).append(sp)
                        .append(totalAmt).append(sp)
                        .append(arr[5].trim()).append(sp)
                        .append(arr[6].trim()).append(sp)
                        .append(highPrice).append(sp)
                        .append(lowPrice).append(sp)
                        .append(tradePrice).append(sp)
                        .append(tradePrice).append(sp)
                        .append("T11").append(sp)
                        .append(new Date().getTime()); //事件时间

                list.add(builder.toString());
            }
            br.close();
        } catch (Exception e) {
            System.err.println("errors :" + e);
        }
        return list;
    }
}