package com.xiaopeng.avro;

import cn.itcast.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

/**
 * demo案例:序列化和反序列化
 */
public class AvroDemo {

    /**
     * 开发步骤:
     * 1.构建对象(三种)
     * 2.封装数据
     * 3.序列化
     * 4.反序列化
     */
    public static void main(String[] args) throws IOException {
        // 1.构建对象(三种)
        User user = new User();
        //2.封装数据
        user.setName("唐三");
        user.setAddress("圣魂村");
        user.setAge(20);

        User user1 = new User("小舞", 20, "星斗大森林");

        User user2 = User.newBuilder()
                .setAge(20)
                .setName("唐浩")
                .setAddress("昊天宗")
                .build();

        //3.序列化
        //定义schema
//        SpecificDatumWriter<User> specificDatumWriter = new SpecificDatumWriter<>(User.class);
//        DataFileWriter<User> fileWriter = new DataFileWriter<>(specificDatumWriter);
//        //写文件演示
//        fileWriter.create(user.getSchema(),new File("avro.txt"));
//        //写数据到文件
//        fileWriter.append(user);
//        fileWriter.append(user1);
//        fileWriter.append(user2);
//        fileWriter.close();

        //反序列化
        //定义schema
        SpecificDatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> res = new DataFileReader<>(new File("avro.txt"), datumReader);
        for (User user3 : res) {
            System.out.println("反序列数据:"+user3);
        }

        res.close();
    }

}
