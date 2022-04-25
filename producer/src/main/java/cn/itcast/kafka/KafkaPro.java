package cn.itcast.kafka;

import cn.itcast.avro.AvroSerializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka生产者对象
 */
//1.创建类，泛型参数继承avro基类
public class KafkaPro<T extends SpecificRecordBase> {

    //新建生产者对象
    KafkaProducer kafkaProducer = new KafkaProducer<>(getProp());
    /**
     * 2.设置生产者参数
     * 3.自定avro序列化
     * 4.添加发送数据方法
     */
    //2.设置生产者参数
    public Properties getProp(){
        Properties properties = new Properties();
        /**
         * bootstrap.servers ：broker地址
         * acks ：0,1和-1
         * retries：重试次数
         * batch.size：批量发送大小默认16384 （16kB）
         * linger.ms： 定时发送1ms
         * buffer.memory: 缓存大小33554432
         * key.serializer：key序列化
         * value.serializer： value序列化
         */
        properties.setProperty("bootstrap.servers","node01:9092");
        properties.setProperty("acks","0");
        properties.setProperty("retries","0");
        properties.setProperty("batch.size","16384");
        properties.setProperty("linger.ms","1");
        properties.setProperty("buffer.memory","33554432");//32Mb
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        //自定义avro序列化
        properties.setProperty("value.serializer", AvroSerializer.class.getName());
        return properties;
    }

    //4.添加发送数据方法
    public void sendData(String topic ,T data){
        kafkaProducer.send(new ProducerRecord(topic,data));
    }


}
