package com.xiaopeng.szse;

import com.xiaopeng.avro.SzseAvro;
import com.xiaopeng.kafka.KafkaPro;

import java.io.DataInputStream;
import java.io.InputStream;
import java.net.Socket;
import java.util.Date;

/**
 * 客户端接收服务端的数据
 */
public class SocketClient {
    //1.创建main方法
    public static void main(String[] args) throws Exception {
        //2.建立socket连接，获取流数据
        Socket socket = new Socket("localhost", 4444);
        InputStream ins = socket.getInputStream();
        DataInputStream result = new DataInputStream(ins);

        KafkaPro kafkaPro = new KafkaPro();
        while (true){
            String str = result.readUTF();
            //3.解析行数据，数据转换
            //创建avsc文件,编译生成avro bean对象,并封装数据到bean对象
            //解析字符串数据,并封装到avro
            SzseAvro szseAvro = transfer(str);
            System.out.println(szseAvro);
            //4.发送kafka
            kafkaPro.sendData("szse",szseAvro);
        }
    }

    private static SzseAvro transfer(String str) {
        //900|399361|国证商业|13794097|37392971|4674.72|4705.73|
        // 5646.88|3764.58|4164.51|4164.51|T11|1621346291167
        String[] arr = str.split("\\|");
        SzseAvro szseAvro = new SzseAvro();
        szseAvro.setMdStreamID(arr[0].trim());
        szseAvro.setSecurityID(arr[1].trim());
        szseAvro.setSymbol(arr[2].trim());
        szseAvro.setTradeVolume(Long.valueOf(arr[3].trim()));
        szseAvro.setTotalValueTraded(Long.valueOf(arr[4].trim()));
        szseAvro.setPreClosePx(Double.valueOf(arr[5].trim()));
        szseAvro.setOpenPrice(Double.valueOf(arr[6].trim()));
        szseAvro.setHighPrice(Double.valueOf(arr[7].trim()));
        szseAvro.setLowPrice(Double.valueOf(arr[8].trim()));
        szseAvro.setTradePrice(Double.valueOf(arr[9].trim()));
        szseAvro.setClosePx(Double.valueOf(arr[10].trim()));
        szseAvro.setTradingPhaseCode("T01");//实时阶段标志,表示正在交易期间
        szseAvro.setTimestamp(new Date().getTime()); //事件时间,后面流处理程序,划分时间窗口使用
        return szseAvro;
    }

}
