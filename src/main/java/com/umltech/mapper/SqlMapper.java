package com.umltech.mapper;

import com.umltech.bean.Alarm;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

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
 * <tr><td>1.0</td><td>2019/3/12</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
@Mapper
public interface SqlMapper {

    /**
     * 添加报警表,一天一张表
     *
     * @param tableName
     * @param alarmRows
     * @return
     */
    int batchInsert(@Param("tableName") String tableName, @Param("alarmRows") List<Alarm> alarmRows);

    /**
     * 创建报警表
     *
     * @param tableName
     * @return
     */
    int createTable(@Param("tableName") String tableName);

    /**
     * 删除报警表
     *
     * @param tableName
     */
    void deleteAll(@Param("tableName") String tableName);
}
