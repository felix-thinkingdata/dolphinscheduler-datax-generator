package org.apache.dolphinscheduler.api.service.impl;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

import org.apache.dolphinscheduler.api.dto.dataxmeta.PluginColumn;
import org.apache.dolphinscheduler.api.service.DataxColumnGenerateService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataxHdfsReaderColumnGenerateServiceImpl extends DataxColumnGenerateService {


    private static final Logger logger = LoggerFactory.getLogger(DataxHdfsReaderColumnGenerateServiceImpl.class);

    @Override
    public List<PluginColumn> getPluginColumns() {

        String defaultFS = getPluginParamInfo().get("defaultFS");
        String demofile = getPluginParamInfo().get("demoFile");
        String fileType = getPluginParamInfo().get("fileType");
        return getPluginColumns(defaultFS, demofile, fileType);

        //return pluginColumns;
    }

    private List<PluginColumn> getPluginColumns(String defaultFS, String filePath, String fileType) {

        if (FileType.PAR.name().equals(fileType)) {
            return getParquetPluginColumn(defaultFS, filePath);
        }
        return null;
    }

    /**
     * describe: get parquet columns
     * creat_user: felix@thinkingdata.cn
     * creat_date: 2020/12/24
     * creat_time: 11:30 上午
     **/
    private List<PluginColumn> getParquetPluginColumn(String defaultFS, String filePath) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", defaultFS);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.trash.interval", "4320");

        ParquetMetadata metaData;
        Path path = new Path(filePath);
        List<PluginColumn> pluginColumns = new ArrayList<>();
        FileSystem fs;
        try {
            fs = path.getFileSystem(conf);
            Path file;
            if (fs.isDirectory(path)) {
                FileStatus[] statuses = fs.listStatus(path, HiddenFileFilter.INSTANCE);
                if (statuses.length == 0) {
                    throw new RuntimeException("Directory " + path.toString() + " is empty");
                }
                file = statuses[0].getPath();
            } else {
                file = path;
            }
            metaData = ParquetFileReader.readFooter(conf, file, NO_FILTER);

            MessageType schema = metaData.getFileMetaData().getSchema();

            for (int i = 0; i < schema.getFields().size(); i++) {
                PluginColumn pluginColumn = new PluginColumn();
                Type field = schema.getFields().get(i);
                pluginColumn.setName(field.getName());
                pluginColumn.setType(field.asPrimitiveType().getPrimitiveTypeName().name());
                pluginColumn.setIndex(i);
                pluginColumns.add(pluginColumn);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return pluginColumns;
    }


    private enum FileType {
        PAR,
        ORC
    }

}
