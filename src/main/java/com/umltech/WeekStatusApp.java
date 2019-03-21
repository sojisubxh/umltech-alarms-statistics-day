package com.umltech;

import com.umltech.outputFormat.SparkOutputFormat;
import com.umltech.util.Constants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p>
 * ----------------------------------------------------------------------------- <br>
 * 工程名 ：umltech-alarms-statistics-week <br>
 * 功能：<br>
 * 描述：<br>
 * 授权 : (C) Copyright (c) 2016<br>
 * 公司 : 北京博创联动科技有限公司<br>
 * ----------------------------------------------------------------------------- <br>
 * 修改历史<br>
 * <table width="432" border="1">
 * <tr><td>版本</td><td>时间</td><td>作者</td><td>改变</td></tr>
 * <tr><td>1.0</td><td>2019/3/14</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
public class WeekStatusApp implements Serializable {

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) throws ParseException {
        SparkConf sc = new SparkConf();
        if (args.length < 1) {
            sc.setAppName("alarm.week.status").setMaster("local[*]");
            args = argsForTest();
        }

        String inputDir = args[0];
        String outputDir = args[1];
        String date = args[2];

        JavaSparkContext jsc = new JavaSparkContext(sc);
        JobConf jobConf = new JobConf();
        jobConf.set(Constants.FILE_NAME, date);

        // 查询前一天报警统计数据结果rdd
        JavaRDD<String> javaRdd = jsc.textFile(inputDir);

        JavaPairRDD<String, String> keyValuesRdd = javaRdd
                .mapPartitionsToPair((PairFlatMapFunction<Iterator<String>, String, String>) iterator -> {
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    while (iterator.hasNext()) {
                        String data = iterator.next();
                        list.add(new Tuple2<>("1", data));
                        System.out.println(data);
                    }
                    return list;
                });

        keyValuesRdd.repartition(1).saveAsHadoopFile(outputDir, String.class, String.class, SparkOutputFormat.class, jobConf);
    }

    private static String[] argsForTest() {
        String inputDir = "hdfs://cdh2:8020/spark/can/2019/03/07/part-000001551927600000";
        String outputDir = "hdfs://cdh-master2:8020/data/spark/alarmStatus/day/";
        String date = "20190304";
        return new String[]{inputDir, outputDir, date};
    }
}
