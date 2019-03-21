package com.umltech.comparator;

import cn.hutool.log.StaticLog;

import java.io.Serializable;
import java.util.Comparator;

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
public class MyComparator implements Comparator<String>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(String o1, String o2) {
        String[] keys1 = o1.split(",");
        String[] keys2 = o2.split(",");

        try {
            // 根据gps时间排序,数据过滤时已把gps时间为空的改为TIME,此处不用再处理
            long t1 = Long.parseLong(keys1[1]);
            long t2 = Long.parseLong(keys2[1]);
            if (t1 < t2) {
                return -1;
            } else if (t1 > t2) {
                return 1;
            } else {
                return 0;
            }
        } catch (NumberFormatException e) {
            StaticLog.error("key1 : {},key2 : {}", o1, o2);
            return 0;
        }
    }
}
