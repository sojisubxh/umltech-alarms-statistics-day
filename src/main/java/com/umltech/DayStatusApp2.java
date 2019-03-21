package com.umltech;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.log.StaticLog;
import cn.hutool.setting.dialect.Props;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.umltech.bean.Alarm;
import com.umltech.bean.AlarmDetail;
import com.umltech.comparator.MyComparator;
import com.umltech.mapper.SqlMapper;
import com.umltech.outputFormat.SparkOutputFormat;
import com.umltech.partitioner.NewHashPartitioner;
import com.umltech.util.AlarmCodeUtil;
import com.umltech.util.Constants;
import com.umltech.util.HbaseTool;
import com.uptech.uml.protocol.util.ByteUtils;
import com.xiaoleilu.hutool.log.Log;
import com.xiaoleilu.hutool.log.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.Tuple2;

import java.security.MessageDigest;
import java.util.*;

/**
 * <p>
 * ----------------------------------------------------------------------------- <br>
 * 工程名 ：umltech-alarms-statistics-day <br>
 * 功能：存储报警<br>
 * 描述：<br>
 * 授权 : (C) Copyright (c) 2016<br>
 * 公司 : 北京博创联动科技有限公司<br>
 * ----------------------------------------------------------------------------- <br>
 * 修改历史<br>
 * <table width="432" border="1">
 * <tr><td>版本</td><td>时间</td><td>作者</td><td>改变</td></tr>
 * <tr><td>1.0</td><td>2019/3/7</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
public class DayStatusApp2 {

    private static final Log log = LogFactory.get();

    private static final Props props = new Props("application.properties");
    private static Map<String, Map<String, String>> mc = AlarmCodeUtil.getAlarmCode(props.getProperty("alarm.code.url"));// 内部报警配置map，key就是报警编码19位的
    private static final String CONVERSION_URL = props.getProperty("conversion.url");
    private static HbaseTool hbaseTool = new HbaseTool(props.getStr("hbase.zookeeper.quorum"), props.getStr("zookeeper.znode.parent"), 10);
    private static ApplicationContext ac = new ClassPathXmlApplicationContext("spring-dataSource.xml", "spring-mybatis.xml");
    private static SqlMapper sqlMapper = ac.getBean(SqlMapper.class);

    private static final String FORMAT = "yyyyMMddHHmmss";
    private static final String ALARM_NAME = "故障名称";
    private static final String FAMILY_ALARM = "alarm";// 报警表列簇
    private static final String TABLE_NAME_ALARM = "alarm";// 报警表名

    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
        if (mc.size() < 1) {
            System.out.println("配置文件为空");
            StaticLog.info("配置文件为空");
            System.exit(0);
        }
        if (args.length < 4) {
            sc.setAppName("alarm.day.status").setMaster("local[*]");// 本地模式
            args = argsForTest();
        }

        String inputDir = args[0];// 输入目录
        String outputDir = args[1] + args[3].substring(0, args[3].indexOf("/", 6));// 输出目录
        int numPartitions = Integer.parseInt(args[2]);// 分区数
        String day = args[3].replaceAll("/", "");// 处理日期（精确到天）
        double mileageLimit = Double.parseDouble(args[4]);// 里程限制
        String tableName = "alarm_day_" + day;// mysql表名

        // hdfs config
        JavaSparkContext jsc = new JavaSparkContext(sc);
        JobConf jobConf = new JobConf();
        jobConf.set(Constants.FILE_NAME, day);

        // 查询前一天数据结果rdd
        JavaRDD<String> javaRdd = jsc.textFile(inputDir);
        // 过滤数据
        final JavaRDD<String> filterRdd = javaRdd.filter((Function<String, Boolean>) line -> {
            // 过滤为空
            return !StringUtils.isEmpty(line) && checkData(line, day);
        }).persist(StorageLevel.MEMORY_ONLY_SER());

        // 数据按时间排序后按did聚合
        JavaPairRDD<String, List<String>> keyValuesRdd = filterRdd
                .mapPartitionsToPair((PairFlatMapFunction<Iterator<String>, String, String>) iterator -> {
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    while (iterator.hasNext()) {
                        String data = iterator.next();
                        JSONObject jsonObject = parseJson(data);
                        String key = jsonObject.get(Constants.DID) + "," + getTime(jsonObject);
                        list.add(new Tuple2<>(key, filterData(jsonObject)));
                    }
                    return list;
                }).repartitionAndSortWithinPartitions(new NewHashPartitioner(numPartitions), new MyComparator())
                .mapPartitionsToPair((PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, List<String>>) tuple2Iterator -> {
                    List<Tuple2<String, List<String>>> list = new ArrayList<>();
                    while (tuple2Iterator.hasNext()) {
                        Tuple2<String, String> kv = tuple2Iterator.next();
                        List<String> value = new ArrayList<>();
                        value.add(kv._2());
                        list.add(new Tuple2<>(kv._1().split(",")[0], value));
                    }
                    return list;
                }).reduceByKey((Function2<List<String>, List<String>, List<String>>) (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                }).persist(StorageLevel.MEMORY_ONLY_SER());

        JavaPairRDD<String, String> resultRdd = keyValuesRdd.mapPartitionsToPair((PairFlatMapFunction<Iterator<Tuple2<String, List<String>>>, String, String>) i -> {
            List<Tuple2<String, String>> resultlist = new ArrayList<>();
            while (i.hasNext()) {
                Tuple2<String, List<String>> tuple = i.next();
                List<Alarm> alarms = getAlarmMessage(tuple);
                if (CollectionUtil.isNotEmpty(alarms)) {
                    alarms.forEach(alarm -> resultlist.add(new Tuple2<>(null, JSONObject.toJSONString(alarm))));
                }
            }
            return resultlist;
        }).persist(StorageLevel.MEMORY_ONLY_SER());

        resultRdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, String>>>) partition -> {
            List<Alarm> alarmList = new ArrayList<>();// mysql报警数据
            List<Put> alarms = new ArrayList<>();// hbase报警数据
            while (partition.hasNext()) {
                String value = partition.next()._2();
                alarmList.add(JSONObject.parseObject(value, Alarm.class));

                JSONObject json = JSON.parseObject(value);
                String did = json.getString("did");
                String code = json.getString("code");
                String level = json.getString("level");
                String name = json.getString("name");
                String alarmlist = json.getString("alarmList");

                List<AlarmDetail> list = JSON.parseArray(alarmlist, AlarmDetail.class);
                list.forEach(alarmDetail -> {
                    if (alarmDetail != null) {
                        Put put = new Put(Bytes.toBytes(getMd5String(did) + "_" + alarmDetail.getStartTime() + "_" + code));
                        put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("did"), Bytes.toBytes(did));
                        put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("code"), Bytes.toBytes(code));
                        put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("level"), Bytes.toBytes(level));
                        put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("name"), Bytes.toBytes(name));
                        put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("startTime"), Bytes.toBytes(alarmDetail.getStartTime()));
                        if (StrUtil.isNotEmpty(alarmDetail.getStartLon())) {
                            put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("startLon"), Bytes.toBytes(alarmDetail.getStartLon()));
                        }
                        if (StrUtil.isNotEmpty(alarmDetail.getStartLat())) {
                            put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("startLat"), Bytes.toBytes(alarmDetail.getStartLat()));
                        }
                        if (StrUtil.isNotEmpty(alarmDetail.getStartLon()) && StrUtil.isNotEmpty(alarmDetail.getStartLat())) {
                            String address = HttpUtil.get(CONVERSION_URL.replace("{lon}", alarmDetail.getStartLon()).
                                    replace("{lat}", alarmDetail.getStartLat()));
                            if (StrUtil.isNotEmpty(address)) {
                                put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("startAdderss"), Bytes.toBytes(address.trim()));
                            }
                        }
                        put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("endTime"), Bytes.toBytes(alarmDetail.getEndTime()));
                        if (StrUtil.isNotEmpty(alarmDetail.getEndLon())) {
                            put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("endLon"), Bytes.toBytes(alarmDetail.getEndLon()));
                        }
                        if (StrUtil.isNotEmpty(alarmDetail.getEndLat())) {
                            put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("endLat"), Bytes.toBytes(alarmDetail.getEndLat()));
                        }
                        if (StrUtil.isNotEmpty(alarmDetail.getEndLon()) && StrUtil.isNotEmpty(alarmDetail.getEndLat())) {
                            String address = HttpUtil.get(CONVERSION_URL.replace("{lon}", alarmDetail.getEndLon()).
                                    replace("{lat}", alarmDetail.getEndLat()));
                            if (StrUtil.isNotEmpty(address)) {
                                put.addColumn(Bytes.toBytes(FAMILY_ALARM), Bytes.toBytes("startAdderss"), Bytes.toBytes(address.trim()));
                            }
                        }
                        alarms.add(put);
                    }
                });
            }
            // 存hbase
            if (CollectionUtil.isNotEmpty(alarms)) {
                hbaseTool.put(TABLE_NAME_ALARM, alarms);
                hbaseTool.destroy();
            }
            // 存mysql
            sqlMapper.createTable(tableName);
            sqlMapper.deleteAll(tableName);
            sqlMapper.batchInsert(tableName, alarmList);
        });
        // 存hdfs
        resultRdd.repartition(1).saveAsHadoopFile(outputDir, String.class, String.class, SparkOutputFormat.class, jobConf);
    }

    /**
     * 封装报警数据
     *
     * @param tuple
     * @return
     */
    private static List<Alarm> getAlarmMessage(Tuple2<String, List<String>> tuple) {
        List<Alarm> list = new ArrayList<>();

        String did = tuple._1();// 设备id
        List<String> values = tuple._2();// 数据
        Set<String> preAlarms = new HashSet<>();// 记录前一条信息报警编号
        Map<String, AlarmDetail> tempMap = new HashMap<>();
        Map<String, List<AlarmDetail>> details = new HashMap<>();

        boolean lastLine = false;// 是否为最后一条
        for (int i = 0; i < values.size(); i++) {
            JSONObject resultJson = JSONObject.parseObject(values.get(i));
            if (i == (values.size() - 1)) {
                lastLine = true;
            }
            getAlarmDeatil(resultJson, preAlarms, tempMap, lastLine, details);
        }

        // 封装数据
        details.forEach((key, value) -> {
            Alarm alarm = new Alarm();
            alarm.setDid(did);// 设备编号
            alarm.setCode(key);// 报警编码
            alarm.setTotal(value.size());// 相同类型报警,报警数目
            alarm.setAlarmList(JSONObject.toJSONString(value));// 相同类型报警集合
            alarm.setUpdateTime(new Date());// 最后更新时间

            AlarmDetail detail = value.get(value.size() - 1);// 最后一条报警数据
            if (StrUtil.isNotEmpty(detail.getStartLon())) {
                alarm.setLastLon(Double.parseDouble(detail.getStartLon()));
            } else {
                alarm.setLastLon(null);
            }
            if (StrUtil.isNotEmpty(detail.getStartLat())) {
                alarm.setLastLat(Double.parseDouble(detail.getStartLat()));
            } else {
                alarm.setLastLat(null);
            }
            alarm.setLastTime(DateUtil.parse(detail.getStartTime(), FORMAT));// 最后报警时间
            if (key.length() == 19) {
                alarm.setLevel(new Integer(key.substring(17)));
            } else {
                alarm.setLevel(1);
//                StaticLog.info("报警编码长度格式错误:{}", key);
                log.info("报警编码长度格式错误:{}", key);
            }
            Map<String, String> map = mc.get(key);
            if (map != null) {
                alarm.setName(map.get(ALARM_NAME));
            } else {
                alarm.setName("");
            }
            // 地理位置
            if (alarm.getLastLon() != null && alarm.getLastLat() != null) {
                String address = HttpUtil.get(CONVERSION_URL.replace("{lon}", alarm.getLastLon().toString()).
                        replace("{lat}", alarm.getLastLat().toString()));
                if (StrUtil.isNotEmpty(address)) {
                    alarm.setLastAddress(address.trim());
                }
            }

            list.add(alarm);
        });
        details.clear();
        preAlarms.clear();
        return list;
    }

    /**
     * 分析每条数据生成报警信息
     *
     * @param resultJson 当前数据
     * @param alarmCache 记录前一条信息报警编码
     * @param temp       当前已经开始的报警信息
     * @param lastLine   是否为最后一条
     * @param details    最终报警数据集合
     */
    private static void getAlarmDeatil(JSONObject resultJson, Set<String> alarmCache, Map<String, AlarmDetail> temp, boolean lastLine, Map<String, List<AlarmDetail>> details) {
        List<String> alarmList = new ArrayList<>();// 故障列表
        // 判断当前信息是否有告警数据
        if (resultJson.containsKey(Constants.ALARMS)) {
            String alarms = resultJson.getString(Constants.ALARMS);
            alarmList.addAll(Arrays.asList(alarms.split("\\|")));
        }
        // 报警结束
        if (CollectionUtil.isNotEmpty(alarmCache)) {
            List<String> endAlarm = new ArrayList<>();
            alarmCache.forEach(cache -> {
                if (!alarmList.contains(cache)) {
                    // 报警缓存中不存在当前报警,则报警结束
                    AlarmDetail ad = temp.get(cache);
                    List<AlarmDetail> faultList = details.get(cache);
                    if (CollectionUtil.isEmpty(faultList)) {
                        faultList = new ArrayList<>();
                    }
                    faultList.add(ad);
                    details.put(cache, faultList);
                    endAlarm.add(cache);
                }
            });
            endAlarm.forEach(ea -> {
                temp.remove(ea);
                alarmCache.remove(ea);
            });
        }
        // 报警开始
        if (CollectionUtil.isNotEmpty(alarmList)) {
            alarmList.forEach(alarm -> {
                AlarmDetail ad;

                // 缓存中不存在的报警为新报警
                if (!alarmCache.contains(alarm)) {
                    ad = new AlarmDetail();
                    ad.setStartLon(resultJson.getString(Constants.GPS_LON));
                    ad.setEndLon(resultJson.getString(Constants.GPS_LON));
                    ad.setStartLat(resultJson.getString(Constants.GPS_LAT));
                    ad.setEndLat(resultJson.getString(Constants.GPS_LAT));
                    ad.setStartTime(resultJson.getString(Constants.GPS_TIME));
                    ad.setEndTime(resultJson.getString(Constants.GPS_TIME));
                    temp.put(alarm, ad);
                    alarmCache.add(alarm);
                } else {
                    // 连续报警更新结束状态
                    ad = temp.get(alarm);
                    ad.setEndLon(resultJson.getString(Constants.GPS_LON));
                    ad.setEndLat(resultJson.getString(Constants.GPS_LAT));
                    ad.setEndTime(resultJson.getString(Constants.GPS_TIME));
                }
            });
        }
        // 最后一条数据结束报警
        if (lastLine) {
            if (CollectionUtil.isNotEmpty(alarmCache)) {
                alarmCache.forEach(cache -> {
                    AlarmDetail ad = temp.get(cache);
                    List<AlarmDetail> faultList = details.get(cache);
                    if (CollectionUtil.isEmpty(faultList)) {
                        faultList = new ArrayList<>();
                    }
                    faultList.add(ad);
                    details.put(cache, faultList);
                });
            }
            alarmCache.clear();
        }
    }

    /**
     * 检查数据准确性
     *
     * @param line 每行数据
     * @param day  对比日期
     * @return
     */
    private static boolean checkData(String line, String day) {
        try {
            // line:SUBMIT$1$BYN1010170316175$REALTIME$TIME:20190301102957,3001:1,3002:1
            String[] msg = line.trim().split("\\$");
            // 判断数据长度 5
            if (msg.length < 5) {
                StaticLog.info("数据长度不够:" + line);
                return false;
            }
            // 数据类型
            if (!msg[3].equals(Constants.REALTIME) && !msg[3].equals(Constants.HISTORY)) {
                return false;
            }
            // 过滤上报日期
//            if (msg[4].contains("TIME:")) {
//                String time = msg[4].substring(msg[4].indexOf("TIME:") + "TIME:".length(), msg[4].indexOf("TIME:") + 13);
//                if (!time.substring(0, 8).equals(day)) {
//                    StaticLog.info("上报日期错误:" + line);
//                    return false;
//                }
//            }
        } catch (Exception e) {
            StaticLog.error(e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 内部协议格式数据转JSON对象
     *
     * @param data 内部协议数据
     * @return
     */
    private static JSONObject parseJson(String data) {
        // SUBMIT$1$BYN1010170316175$REALTIME$TIME:20190301102957,3001:1,3002:1
        JSONObject json = new JSONObject();
        String[] reps = data.split("\\$");
        // 数据类型
        json.put(Constants.DATE_TYPE, reps[3]);
        // 设备id
        json.put(Constants.DID, reps[2]);

        String[] array = reps[4].split(",");
        Arrays.asList(array).forEach(kv -> {
            if (StringUtils.isNotBlank(kv)) {
                String[] v = kv.split(":");
                if (v.length == 2) {
                    json.put(v[0], v[1]);
                }
            }
        });
        return json;
    }

    private static String filterData(JSONObject jsonObject) {
        JSONObject data = new JSONObject();
        // GPS时间
        if (jsonObject.containsKey(Constants.GPS_TIME)) {
            data.put(Constants.GPS_TIME, jsonObject.getString(Constants.GPS_TIME));
        } else {
            data.put(Constants.GPS_TIME, jsonObject.getString(Constants.TIME));
        }
        // 报警信息
        StringBuilder alarms = new StringBuilder();
        if (jsonObject.containsKey(Constants.ALARMS)) {
            String[] alarmArray = jsonObject.getString(Constants.ALARMS).split("\\|");
            for (String alarm : alarmArray) {
                alarms.append(alarm).append("|");
            }
        }
        if (jsonObject.containsKey(Constants.ALARMS_NEW)) {
            String[] alarmArray = jsonObject.getString(Constants.ALARMS_NEW).split("\\|");
            for (String alarm : alarmArray) {
                alarms.append(alarm).append("|");
            }
        }
        if (jsonObject.containsKey(Constants.REES_DEVICE_FAULT_CODELIST)) {
            String[] alarmArray = jsonObject.getString(Constants.REES_DEVICE_FAULT_CODELIST).split("\\|");
            for (String alarm : alarmArray) {
                alarms.append(alarm).append("|");
            }
        }
        if (jsonObject.containsKey(Constants.DRIVE_MOTOR_FAULT_CODELIST)) {
            String[] alarmArray = jsonObject.getString(Constants.DRIVE_MOTOR_FAULT_CODELIST).split("\\|");
            for (String alarm : alarmArray) {
                alarms.append(alarm).append("|");
            }
        }
        if (jsonObject.containsKey(Constants.ENGINE_FAULTLIST)) {
            String[] alarmArray = jsonObject.getString(Constants.ENGINE_FAULTLIST).split("\\|");
            for (String alarm : alarmArray) {
                alarms.append(alarm).append("|");
            }
        }
        if (StringUtils.isNotBlank(alarms.toString())) {
            data.put(Constants.ALARMS, alarms.toString());
        }
        // 经纬度
        data.put(Constants.GPS_LON, jsonObject.getString(Constants.GPS_LON));
        data.put(Constants.GPS_LAT, jsonObject.getString(Constants.GPS_LAT));
        // 总里程
        data.put(Constants.TOTAL_MILEAGE, jsonObject.getString(Constants.TOTAL_MILEAGE));
        return data.toJSONString();
    }

    /**
     * 获取gps时间,如果没报则用系统接收时间
     *
     * @param jsonObject
     * @return
     */
    private static String getTime(JSONObject jsonObject) {
        if (jsonObject.containsKey(Constants.GPS_TIME)) {
            return jsonObject.getString(Constants.GPS_TIME);
        } else {
            return jsonObject.getString(Constants.TIME);
        }
    }

    public static String getMd5String(String did) {
        String md5did = "";
        try {
            byte[] bytes = MessageDigest.getInstance("MD5").digest(did.getBytes());
            md5did = ByteUtils.getHexByteString(bytes).toLowerCase();
        } catch (Exception e) {
        }
        return md5did;
    }

    private static String[] argsForTest() {
//        String inputDir = "hdfs://cdh2:8020/spark/can/2019/03/07/part-000001551927600000";
        String inputDir = "hdfs://cdh2:8020/spark/can/2019/03/06/part-000151552875420000";
        String outputDir = "hdfs://cdh-master2:8020/data/spark/alarmStatus/day";
        String numPartitions = "1";
        String day = "/2019/03/06/";
        String mileageLimit = "1500";
        return new String[]{inputDir, outputDir, numPartitions, day, mileageLimit};
    }
}
