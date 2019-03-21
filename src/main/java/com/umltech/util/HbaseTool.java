package com.umltech.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.StaticLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import sunyu.toolkit.hbase.pojo.HbaseColumn;
import sunyu.toolkit.hbase.pojo.HbaseFilter;
import sunyu.toolkit.hbase.pojo.HbaseRow;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * ----------------------------------------------------------------------------- <br>
 * 工程名 ：umltech-alarms-statistics-day <br>
 * 功能：hbase工具类<br>
 * 描述：<br>
 * 授权 : (C) Copyright (c) 2016<br>
 * 公司 : 北京博创联动科技有限公司<br>
 * ----------------------------------------------------------------------------- <br>
 * 修改历史<br>
 * <table width="432" border="1">
 * <tr><td>版本</td><td>时间</td><td>作者</td><td>改变</td></tr>
 * <tr><td>1.0</td><td>2019/3/13</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
public class HbaseTool implements Serializable {
    public static final String firstVisibleAscii = "!";//ascii 第一个可见字符
    public static final String lastVisibleAscii = "~";//ascii 最后一个可见字符

    private Connection connection;//hbase链接

    /**
     * 初始化
     *
     * @param zookeeperAddress zookeeper地址(cdh0:2181,cdh1:2181,cdh2:2181)
     * @param znodeParent      znode parent(/hbase)
     * @param threadSize       最大线程数
     */
    public HbaseTool(String zookeeperAddress, String znodeParent, int threadSize) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperAddress);
        config.set("hbase.zookeeper.znode.parent", znodeParent);
        config.set("hbase.encoding", "UTF-8");
        try {
            connection = ConnectionFactory.createConnection(config, ThreadUtil.newExecutor(threadSize));
            StaticLog.info("创建 Hbase 链接成功 [{} {} {}]", zookeeperAddress, znodeParent, threadSize);
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 关闭链接
     */
    public void destroy() {
        if (!connection.isClosed()) {
            try {
                connection.close();
                StaticLog.info("关闭 Hbase 链接成功");
            } catch (IOException e) {
                StaticLog.error(e);
            }
        }
    }

    /**
     * 把字符串最后一个字符的ascii向前挪一位
     *
     * @param str 原字符串
     * @return 新字符串
     */
    public static String lastCharAsciiSubOne(String str) {
        if (StrUtil.isBlank(str)) {
            return "";
        }
        char[] ca = str.toCharArray();
        ca[ca.length - 1] = (char) (ca[ca.length - 1] - 1);
        return new String(ca);
    }

    /**
     * 把字符串最后一个字符的ascii向后挪一位
     *
     * @param str 原字符串
     * @return 新字符串
     */
    public static String lastCharAsciiAddOne(String str) {
        if (StrUtil.isBlank(str)) {
            return "";
        }
        char[] ca = str.toCharArray();
        ca[ca.length - 1] = (char) (ca[ca.length - 1] + 1);
        return new String(ca);
    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public void deleteTable(String tableName) {
        try (Admin admin = connection.getAdmin();) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param familyName
     */
    public void createTable(String tableName, String familyName) {
        createTable(tableName, familyName, null);
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param familyName
     * @param timeToLive 列簇超时时间，单位秒
     */
    public void createTable(String tableName, String familyName, Integer timeToLive) {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        hTableDescriptor.setCompactionEnabled(true);
        HColumnDescriptor family = new HColumnDescriptor(familyName);
        family.setMaxVersions(1);//设置数据保存的最大版本数
        family.setCompressionType(Compression.Algorithm.SNAPPY);//设置压缩
        if (timeToLive != null) {
            family.setTimeToLive(timeToLive);
        }
        hTableDescriptor.addFamily(family);
        try (Admin admin = connection.getAdmin();) {
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 禁用表
     *
     * @param tableName
     */
    public void disableTable(String tableName) {
        try (Admin admin = connection.getAdmin();) {
            admin.disableTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 启用表
     *
     * @param tableName
     */
    public void enableTable(String tableName) {
        try (Admin admin = connection.getAdmin();) {
            admin.enableTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 修改表
     *
     * @param tableName
     */
    public void modifyTable(String tableName) {
        modifyTable(tableName, null);
    }

    /**
     * 修改表
     *
     * @param tableName
     * @param timeToLive 列簇超时时间，单位秒
     */
    public void modifyTable(String tableName, Integer timeToLive) {
        try (Admin admin = connection.getAdmin(); Table table = connection.getTable(TableName.valueOf(tableName))) {
            disableTable(tableName);//禁用表
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();//获得表描述
            hTableDescriptor.setCompactionEnabled(true);
            hTableDescriptor.getFamilies().forEach(hColumnDescriptor -> {
                hColumnDescriptor.setMaxVersions(1);//设置数据保存的最大版本数
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);//设置压缩
                if (timeToLive != null) {
                    hColumnDescriptor.setTimeToLive(timeToLive);
                } else {
                    hColumnDescriptor.setTimeToLive(2147483647);
                }
            });
            admin.modifyTable(TableName.valueOf(tableName), hTableDescriptor);//修改表
        } catch (IOException e) {
            StaticLog.error(e);
        } finally {
            enableTable(tableName);
        }
    }

    /**
     * 转换result为map
     *
     * @param result
     * @return
     */
    private HbaseRow toHbaseRow(String tableName, Result result) {
        HbaseRow hbaseRow = new HbaseRow(tableName, Bytes.toString(result.getRow()));
        for (Cell cell : result.rawCells()) {//循环所有列
            String value = Bytes.toString(CellUtil.cloneValue(cell));//列值
            if (StrUtil.isNotBlank(value)) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));// 获取当前列的familyName
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));//列名
                hbaseRow.getColumnList().add(new HbaseColumn(family, column, value));
            }
        }
        return hbaseRow;
    }

    /**
     * 通过rowKey获取数据
     *
     * @param tableName
     * @param rowKey
     * @return
     */
    public HbaseRow get(String tableName, String rowKey) {
        try (Table table = connection.getTable(TableName.valueOf(tableName));) {
            return toHbaseRow(tableName, table.get(new Get(Bytes.toBytes(rowKey))));
        } catch (IOException e) {
            StaticLog.error(e);
        }
        return null;
    }

    /**
     * 通过rowKey列表，获得一批数据
     *
     * @param tableName
     * @param rowKeyList
     * @return
     */
    public List<HbaseRow> get(String tableName, List<String> rowKeyList) {
        try (Table table = connection.getTable(TableName.valueOf(tableName));) {
            List<HbaseRow> rows = new ArrayList<>();
            List<Get> getList = new ArrayList<>();
            rowKeyList.forEach(rowKey -> getList.add(new Get(Bytes.toBytes(rowKey))));
            for (Result result : table.get(getList)) {
                rows.add(toHbaseRow(tableName, result));
            }
            return rows;
        } catch (IOException e) {
            StaticLog.error(e);
        }
        return null;
    }


    private Put packagePut(String rowKey, List<HbaseColumn> columnList) {
        Put put = new Put(Bytes.toBytes(rowKey));
        columnList.forEach(hbaseColumn -> put.addColumn(Bytes.toBytes(hbaseColumn.getFamilyName()), Bytes.toBytes(hbaseColumn.getColumnName()), Bytes.toBytes(hbaseColumn.getValue())));
        return put;
    }

    /**
     * 存入一条记录
     *
     * @param hbaseRow
     */
    public void put(HbaseRow hbaseRow) {
        try (Table table = connection.getTable(TableName.valueOf(hbaseRow.getTableName()));) {
            table.put(packagePut(hbaseRow.getRowKey(), hbaseRow.getColumnList()));
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 存入多条记录
     *
     * @param rowList
     */
    public void put(List<HbaseRow> rowList) {
        try (Table table = connection.getTable(TableName.valueOf(rowList.get(0).getTableName()));) {
            List<Put> putList = new ArrayList<>();
            rowList.forEach(hbaseRow -> {
                putList.add(packagePut(hbaseRow.getRowKey(), hbaseRow.getColumnList()));
            });
            table.put(putList);
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    public void put(String tableName, List<Put> puts) {
        try (Table table = connection.getTable(TableName.valueOf(tableName));) {
            table.put(puts);
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 删除一条记录
     *
     * @param tableName
     * @param rowKey
     */
    public void delete(String tableName, String rowKey) {
        try (Table table = connection.getTable(TableName.valueOf(tableName));) {
            table.delete(new Delete(Bytes.toBytes(rowKey)));
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 删除一批记录
     *
     * @param tableName
     * @param rowKeyList
     */
    public void delete(String tableName, List<String> rowKeyList) {
        try (Table table = connection.getTable(TableName.valueOf(tableName));) {
            List<Delete> deleteList = new ArrayList<>();
            rowKeyList.forEach(rowKey -> deleteList.add(new Delete(Bytes.toBytes(rowKey))));
            table.delete(deleteList);
        } catch (IOException e) {
            StaticLog.error(e);
        }
    }

    /**
     * 获取一批数据
     *
     * @param tableName
     * @param familyName
     * @param startRow
     * @param stopRow
     * @param pageSize
     * @param columnList
     * @param filterList
     * @param returnKeyOnly
     * @return
     */
    public List<HbaseRow> scan(String tableName, String familyName
            , String startRow, String stopRow, int pageSize
            , List<String> columnList, List<HbaseFilter> filterList, boolean returnKeyOnly) {
        boolean reverse = startRow.compareTo(stopRow) > 0;//如果startRow大于stopRow，则说明是逆序查询
        try (Table table = connection.getTable(TableName.valueOf(tableName));) {
            List<HbaseRow> rows = new ArrayList<>();
            Scan scan = new Scan();
            FilterList filters = new FilterList();
            List<String> columns = new ArrayList<>();
            if (CollUtil.isNotEmpty(columnList)) {
                columnList.forEach(column -> columns.add(column));
            }
            int fetch = 100;//最大一次RPC返回1000条
            if (pageSize < fetch) {
                fetch = pageSize;
            }
            //    scan.setCaching(fetch);
            scan.setReversed(reverse);//设置正序、逆序查询
            scan.setStartRow(Bytes.toBytes(startRow));
            //默认scan是扫描startRow但不包括stopRow，所以下面做判断，改变stopRow
            if (reverse) {//逆序
                scan.setStopRow(Bytes.toBytes(lastCharAsciiSubOne(stopRow)));
            } else {//正序
                scan.setStopRow(Bytes.toBytes(lastCharAsciiAddOne(stopRow)));
            }
            if (returnKeyOnly) {
                filters.addFilter(new KeyOnlyFilter());//只查询rowKey
            }
            if (CollUtil.isNotEmpty(filterList)) {
                CompareFilter.CompareOp compareOp = null;
                for (HbaseFilter hbaseFilter : filterList) {
                    switch (hbaseFilter.getOperator()) {
                        case EQUAL:
                            compareOp = CompareFilter.CompareOp.EQUAL;
                            break;
                        case GREATER:
                            compareOp = CompareFilter.CompareOp.GREATER;
                            break;
                        case GREATER_OR_EQUAL:
                            compareOp = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                            break;
                        case LESS:
                            compareOp = CompareFilter.CompareOp.LESS;
                            break;
                        case LESS_OR_EQUAL:
                            compareOp = CompareFilter.CompareOp.LESS_OR_EQUAL;
                            break;
                        case NOT_EQUAL:
                            compareOp = CompareFilter.CompareOp.NOT_EQUAL;
                            break;
                        case CONTAINS:
                            compareOp = CompareFilter.CompareOp.EQUAL;
                            break;
                        case NO_CONTAINS:
                            compareOp = CompareFilter.CompareOp.NOT_EQUAL;
                            break;
                    }
                    if (compareOp != null) {
                        switch (hbaseFilter.getFilterType()) {
                            case COLUMN_VALUE_FILTER:
                                SingleColumnValueFilter singleColumnValueFilter;
                                switch (hbaseFilter.getOperator()) {
                                    case CONTAINS:
                                        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(hbaseFilter.getColumnName()), compareOp, new SubstringComparator(hbaseFilter.getValue()));
                                        break;
                                    case NO_CONTAINS:
                                        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(hbaseFilter.getColumnName()), compareOp, new SubstringComparator(hbaseFilter.getValue()));
                                        break;
                                    default:
                                        singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(hbaseFilter.getColumnName()), compareOp, Bytes.toBytes(hbaseFilter.getValue()));
                                        break;
                                }
                                singleColumnValueFilter.setFilterIfMissing(hbaseFilter.isFilterIfMissing());
                                filters.addFilter(singleColumnValueFilter);

                                if (!columns.contains(hbaseFilter.getColumnName())) {
                                    columns.add(hbaseFilter.getColumnName());
                                }
                                break;
                            case ROW_REGEX_FILTER:
                                RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(hbaseFilter.getValue()));//正则过滤器
                                filters.addFilter(rowFilter);
                                break;
                        }
                    }
                }
            }
            if (CollUtil.isNotEmpty(columns)) {
                columns.forEach(columnName -> scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
            }
            if (filters.getFilters().size() > 0) {
                scan.setFilter(filters);//添加过滤器
            }
            for (Result result : table.getScanner(scan).next(pageSize)) {
                rows.add(toHbaseRow(tableName, result));
            }
            if (CollUtil.isNotEmpty(rows)) {
                return rows;
            }
        } catch (IOException e) {
            StaticLog.error(e);
        }
        return null;
    }
}
