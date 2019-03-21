package com.umltech.bean;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>
 * ----------------------------------------------------------------------------- <br>
 * 工程名 ：umltech-alarms-statistics-day <br>
 * 功能：<br>
 * 描述：<br>
 * 授权 : (C) Copyright (c) 2016<br>
 * 公司 : 北京博创联动科技有限公司<br>
 * ----------------------------------------------------------------------------- <br>
 * 修改历史<br>
 * <table width="432" border="1">
 * <tr><td>版本</td><td>时间</td><td>作者</td><td>改变</td></tr>
 * <tr><td>1.0</td><td>2019/3/8</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
public class Alarm implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 设备编号
     */
    private String did;
    /**
     * 内部报警编码
     */
    private String code;
    /**
     * 报警级别
     */
    private Integer level;
    /**
     * 内部报警名称
     */
    private String name;
    /**
     * 相同类型报警，报警数目
     */
    private Integer total;
    /**
     * 相同报警，最后报警时间
     */
    private Date lastTime;
    /**
     * 相同报警，最后位置信息
     */
    private String lastAddress;
    /**
     * 相同报警，最后经度
     */
    private Double lastLon;
    /**
     * 相同报警，最后纬度
     */
    private Double lastLat;
    /**
     * 最后更新时间
     */
    private Date updateTime;
    /**
     * alarmList存储内容
     * [
     * {
     * "startLat": 30.656304,
     * "endLat": 30.656304,
     * "startLon": 104.128904,
     * "endLon": 104.128904,
     * "startTime": "20170725101116",
     * "endTime": "20170725101116"
     * },
     * {}...
     * ]
     */
    private String alarmList;// 不用list类型是因为mybatis保存时出错

    public String getDid() {
        return did;
    }

    public void setDid(String did) {
        this.did = did;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Date getLastTime() {
        return lastTime;
    }

    public void setLastTime(Date lastTime) {
        this.lastTime = lastTime;
    }

    public String getLastAddress() {
        return lastAddress;
    }

    public void setLastAddress(String lastAddress) {
        this.lastAddress = lastAddress;
    }

    public Double getLastLon() {
        return lastLon;
    }

    public void setLastLon(Double lastLon) {
        this.lastLon = lastLon;
    }

    public Double getLastLat() {
        return lastLat;
    }

    public void setLastLat(Double lastLat) {
        this.lastLat = lastLat;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getAlarmList() {
        return alarmList;
    }

    public void setAlarmList(String alarmList) {
        this.alarmList = alarmList;
    }
}
