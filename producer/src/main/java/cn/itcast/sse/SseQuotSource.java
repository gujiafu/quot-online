package cn.itcast.sse;

import cn.itcast.avro.SseAvro;
import cn.itcast.util.DownloadUtil;
import cn.itcast.util.TimeUtil;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.io.*;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 沪市行情数据采集
 */
// 1.实现自定义source接口
public class SseQuotSource extends AbstractSource implements PollableSource, Configurable {

    private String host;
    private Integer port;
    private String userName;
    private String password;
    private String ftpDirectory;
    private String fileName;
    private String localDirectory;
    private Integer delay;
    private Integer corePoolSize;
    private Integer maxPoolSize;
    private Long keepAliveTime;
    private Integer capacity;
    ThreadPoolExecutor threadPoolExecutor;

    /**
     * 1.实现自定义source接口
     * 2.初始化参数（初始化source参数和线程池）
     * 3.判断是否是交易时间段
     * 4.异步处理
     * 5.设置延时时间
     */
    //flume数据采集的时候需要三个组件配合:source(采集数据),channel(管道),sink(输出数据)
    //不断的调用此方法
    @Override
    public Status process() throws EventDeliveryException {

        // 3.判断是否是交易时间段
        long time = new Date().getTime();
        if (time < TimeUtil.endTime && time > TimeUtil.startTime) { //保证处理的数据时间都是在正常连续交易期间的

            //4.异步处理
            threadPoolExecutor.execute(new AysncTask());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return Status.READY;
        }

        return Status.BACKOFF;
    }

    @Override
    public void configure(Context context) {
        //2.初始化参数（初始化source参数和线程池）
        //是从配置文件里面获取的
        host = context.getString("host");
        port = context.getInteger("port");
        userName = context.getString("userName");
        password = context.getString("password");
        ftpDirectory = context.getString("ftpDirectory");
        fileName = context.getString("fileName");
        localDirectory = context.getString("localDirectory");
        delay = context.getInteger("delay");
        corePoolSize = context.getInteger("corePoolSize");
        maxPoolSize = context.getInteger("maxPoolSize");
        keepAliveTime = context.getLong("keepAliveTime");
        capacity = context.getInteger("capacity");
        //线程池
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new ArrayBlockingQueue<>(capacity));
    }


    private class AysncTask implements Runnable {
        @Override
        public void run() {
            /**
             * 1.创建异步线程task
             * 2.下载行情文件
             * 3.解析并发送数据
             *   数据转换成avro
             *   数据序列化
             * 4.发送数据到channel
             */
            //下载ftp文件到本地
            boolean status = DownloadUtil.download(host, port, userName, password, ftpDirectory, localDirectory);
            //如果返回true,说明文件下载成功
            if (status) {
                //解析文件(本地)
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(localDirectory + "/" + fileName))));
                    String str;
                    int i =0;
                    while ((str = br.readLine()) != null){
                        String[] arr = str.split("\\|");
                        if(i == 0){
                            String statusCode = arr[8];
                            if(statusCode.startsWith("E")){//不是我们所需要的数据
                                break;
                            }
                        }else{
                            //解析每一行
                            SseAvro sseAvro = transfer(arr);
                            //数据序列化
                            byte[] bte = serializer(sseAvro);
                            //发送channel(管道)
                            getChannelProcessor().processEvent(EventBuilder.withBody(bte));
                        }
                        i++;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private byte[] serializer(SseAvro sseAvro) {
            //定义规范
            SpecificDatumWriter<SseAvro> specificDatumWriter = new SpecificDatumWriter<>(sseAvro.getSchema());
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            //获取编码对象
            BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bos, null);
            try {
                specificDatumWriter.write(sseAvro,binaryEncoder);////将data数据转换成二进制,封装到输出流
            } catch (IOException e) {
                e.printStackTrace();
            }
            return bos.toByteArray();
        }

        private SseAvro transfer(String[] arr) {
            SseAvro sseAvro = new SseAvro();
            sseAvro.setMdStreamID(arr[0].trim());
            sseAvro.setSecurityID(arr[1].trim());
            sseAvro.setSymbol(arr[2].trim());
            sseAvro.setTradeVolume(Long.valueOf(arr[3].trim()));
            sseAvro.setTotalValueTraded(Long.valueOf(arr[4].trim()));
            sseAvro.setPreClosePx(Double.valueOf(arr[5].trim()));
            sseAvro.setOpenPrice(Double.valueOf(arr[6].trim()));
            sseAvro.setHighPrice(Double.valueOf(arr[7].trim()));
            sseAvro.setLowPrice(Double.valueOf(arr[8].trim()));
            sseAvro.setTradePrice(Double.valueOf(arr[9].trim()));
            sseAvro.setClosePx(Double.valueOf(arr[10].trim()));
            sseAvro.setTradingPhaseCode("T01");//实时阶段标志,表示正在交易期间
            sseAvro.setTimestamp(new Date().getTime()); //事件时间,后面流处理程序,划分时间窗口使用
            return sseAvro;
        }

    }
}
