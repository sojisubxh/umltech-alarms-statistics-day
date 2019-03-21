package com.umltech.util;

/**
 * <p>
 * ----------------------------------------------------------------------------- <br>
 * 工程名 ：umltech-alarms-statistics-day <br>
 * 功能：常量类<br>
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
public class Constants {

    /** 文件名称 */
    public static String FILE_NAME = "fileName";

    /** 数据类型(HISTORY, REALTIME ......) */
    public static final String DATE_TYPE = "DATE_TYPE";

    /** 采集时间 */
    public static final String TIME = "TIME";

    /** 设备编号 */
    public static final String DID = "did";

    /** 实时数据 */
    public static final String REALTIME = "REALTIME";

    /** 补发数据 */
    public static final String HISTORY = "HISTORY";

    /** GPS时间 */
    public static final String GPS_TIME = "3014";

    /** 报警字段 2810 */
    public static final String ALARMS = "2810";

    /** 通用报警标志 2802 */
    public static final String ALARMS_NEW = "2802";

    /** 可充电储能装置故障代码列表 2804 */
    public static final String REES_DEVICE_FAULT_CODELIST = "2804";

    /** 驱动电机故障代码列表 2806 */
    public static final String DRIVE_MOTOR_FAULT_CODELIST = "2806";

    /** 发动机故障列表 2808 */
    public static final String ENGINE_FAULTLIST = "2808";

    /** GPS纬度 2603 */
    public static final String GPS_LAT = "2603";

    /** GPS经度 2602 */
    public static final String GPS_LON = "2602";

    /** 瞬时总里程 2205 */
    public static final String TOTAL_MILEAGE = "2205";
}
