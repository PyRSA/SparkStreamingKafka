package com.summer.sparkstream;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.storage.StorageLevel;

import java.util.regex.Pattern;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.spark.broadcast.Broadcast;

public class SparkKryoSerializer {
    private static final Pattern SPACE = Pattern.compile(" ");

    // This is our custom class we will configure Kyro to serialize
    static class tmp1 implements java.io.Serializable {
        public int total_;
        public int num_;
    }

    static class tmp2 implements java.io.Serializable {
        public tmp2(String ss) {
            s = ss;
        }

        public String s;
    }

    public static class toKryoRegistrator implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(tmp1.class, new FieldSerializer(kryo, tmp1.class));  //在Kryo序列化库中注册自定义的类
            kryo.register(tmp2.class, new FieldSerializer(kryo, tmp2.class));  //在Kryo序列化库中注册自定义的类
        }
    }

    public static void readToBuffer(StringBuffer buffer, String filePath) throws IOException {
        InputStream is = new FileInputStream(filePath);
        String line; // 用来保存每行读取的内容
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        line = reader.readLine(); // 读取第一行
        while (line != null) { // 如果 line 为空说明读完了
            buffer.append(line); // 将读到的内容添加到 buffer 中
            buffer.append("\n"); // 添加换行符
            line = reader.readLine(); // 读取下一行
        }
        reader.close();
        is.close();
    }

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("basicavgwithkyro");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");  //使用Kryo序列化库
        conf.set("spark.kryo.registrator", toKryoRegistrator.class.getName());       //在Kryo序列化库中注册自定义的类集合
        conf.set("spark.rdd.compress", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        StringBuffer sb = new StringBuffer();
        SparkKryoSerializer.readToBuffer(sb, args[0]);
        final Broadcast<tmp2> stringBV = sc.broadcast(new tmp2(sb.toString()));

        JavaRDD<String> rdd1 = sc.textFile(args[1]);
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });


        JavaRDD<Integer> rdd3 = rdd2.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) {
                String length = stringBV.value().s;  //只是为了使用广播变量stringBV，没有实际的意义
                String tmp = length;                 //只是为了使用广播变量stringBV，没有实际的意义
                return s.length();
            }
        });

        JavaRDD<tmp1> rdd4 = rdd3.map(new Function<Integer, tmp1>() {
            @Override
            public tmp1 call(Integer x) {
                tmp1 a = new tmp1();  //只是为了将rdd4中的元素类型转换为tmp1类型的对象，没有实际的意义
                a.total_ += x;
                a.num_ += 1;
                return a;
            }
        });

        rdd4.persist(StorageLevel.MEMORY_ONLY_SER());  //将rdd4以序列化的形式缓存在内存中，因为其元素是tmp1对象，所以使用Kryo的序列化方式缓存
        System.out.println("the count is " + rdd4.count());

        while (true) {
        }  //调试命令，只是用来将程序挂住，方便在Driver 4040的WEB UI中观察rdd的storage情况
        //sc.stop();
    }
}
