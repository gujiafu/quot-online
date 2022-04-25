package cn.itcast.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;

//1.创建泛型反序列化类实现反序列化接口
public class AvroDeserializerSchema<T> implements DeserializationSchema<T> {

    /**
     * 1.创建泛型反序列化类实现反序列化接口
     * 2.创建构造方法
     * 3.avro反序列化数据
     * 4.获取反序列化数据类型
     */
    //2.创建构造方法
    String topicName;

    public AvroDeserializerSchema(String topicName) {
        this.topicName = topicName;
    }

    //反序列化
    @Override
    public T deserialize(byte[] message) throws IOException {
        //定义反序列化规范
        SpecificDatumReader<T> specificDatumReader = null;
        if (topicName.equals("sse")) { //说明,数据来自于沪市,需要反序列化成SseAvro
            specificDatumReader = (SpecificDatumReader<T>) new SpecificDatumReader<SseAvro>(SseAvro.class);
        } else {
            specificDatumReader = (SpecificDatumReader<T>) new SpecificDatumReader<SzseAvro>(SzseAvro.class);
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(message);//输入流
        BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(bis, null);
        T read = specificDatumReader.read(null, binaryDecoder);
        return read;
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    //反序列化的数据类型
    @Override
    public TypeInformation<T> getProducedType() {
        TypeInformation<T> of = null;
        if (topicName.equals("sse")) {
            of = (TypeInformation<T>) TypeInformation.of(SseAvro.class);
        } else {
            of = (TypeInformation<T>) TypeInformation.of(SzseAvro.class);
        }
        return of;
    }


}
