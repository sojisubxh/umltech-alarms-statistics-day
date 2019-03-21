package com.umltech.outputFormat;

import com.umltech.util.Constants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

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
public class SparkOutputFormat extends MultipleTextOutputFormat<String, String> {

    @Override
    protected String generateFileNameForKeyValue(String key, String value, String name) {
        return name;
    }

    @Override
    protected RecordWriter<String, String> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3) throws IOException {
        return super.getBaseRecordWriter(fs, job, job.get(Constants.FILE_NAME), arg3);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        Path outDir = getOutputPath(job);
        if (outDir == null && job.getNumReduceTasks() != 0) {
            throw new InvalidJobConfException("Output directory not set in JobConf.");
        }
        if (outDir != null) {
            FileSystem fs = outDir.getFileSystem(job);
            // normalize the output directory
            outDir = fs.makeQualified(outDir);
            setOutputPath(job, outDir);
            // get delegation token for the outDir's file system
            TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{outDir}, job);
        }
    }

}
