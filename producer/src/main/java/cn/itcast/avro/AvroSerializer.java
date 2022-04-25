package cn.itcast.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
// 1.泛型参数继承avro基类，实现序列化接口
public class AvroSerializer <T extends SpecificRecordBase> implements Serializer<T> {

    /**
     *
     *  2.重写序列化方法
     *  3.新建字节数组输出流对象
     *  4.获取二进制对象BinaryEncoder
     *  5.输出数据
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }
    // 2.重写序列化方法
    @Override
    public byte[] serialize(String topic, T data) {
        //定义schema规范
        SpecificDatumWriter<T> specificDatumWriter = new SpecificDatumWriter<>(data.getSchema());
        //3.新建字节数组输出流对象
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        //4.二进制编码对象
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bos, null);
        try {
            specificDatumWriter.write(data,binaryEncoder); //将data数据转换成二进制,封装到输出流
        } catch (IOException e) {
            e.printStackTrace();
        }
        //5.输出数据
        return bos.toByteArray();
    }

    @Override
    public void close() {

    }

}
