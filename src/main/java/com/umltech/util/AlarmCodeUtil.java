package com.umltech.util;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * <tr><td>1.0</td><td>2019/3/7</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
public class AlarmCodeUtil {
    private static Map<String, Map<String, String>> mc = new HashMap<>();

    public static Map<String, Map<String, String>> getAlarmCode(String url) {
        if (mc.isEmpty()) {
            Document document;
            try {
                document = new SAXReader().read(url);
                List<Node> conf = document.selectNodes("/java/object/void/object");
                conf.forEach(node -> {
                    Map<String, String> m = new HashMap<>();
                    List<Node> nodes = node.selectNodes("void");
                    nodes.forEach(node1 -> {
                        List<Node> ns = node1.selectNodes("string");
                        m.put(ns.get(0).getStringValue(), ns.get(1).getStringValue());
                    });
                    mc.put(m.get("故障编码"), m);
                });
            } catch (DocumentException e) {
                e.printStackTrace();
            }
        }
        return mc;
    }
}
