package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.dto.dataxmeta.PluginColumn;
import org.apache.dolphinscheduler.api.service.DataxColumnGenerateService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataxMysqlReaderColumnGenerateServiceImpl extends DataxColumnGenerateService {

    private static final Logger logger = LoggerFactory.getLogger(DataxMysqlReaderColumnGenerateServiceImpl.class);

    /**
     * describe: 要实现两种一种是sql，一种是表名
     * creat_user: felix@thinkingdata.cn
     * creat_date: 2020/12/31
     * creat_time: 1:26 下午
     **/
    @Override
    public List<PluginColumn> getPluginColumns() {

        String jdbcUrl = getPluginParamInfo().get("jdbcUrl");
        String userName = getPluginParamInfo().get("userName");
        String password = getPluginParamInfo().get("password");
        String jdbcDriver = getPluginParamInfo().get("jdbcDriver");

        String tableName = getPluginParamInfo().get("tableName");

        String metaSql = String.format("SELECT COLUMN_NAME , COLUMN_TYPE , DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME ='%s'", tableName);
        logger.info("meta sql  : {}", metaSql);
        logger.info("jdbcUrl : {} ,userName : {},password :{} ,jdbcDriver : {}", jdbcUrl, userName, password, jdbcDriver);
        List<PluginColumn> pluginColumns = new ArrayList<>();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, userName,
                password);
             PreparedStatement stmt = connection.prepareStatement(metaSql);
             ResultSet resultSet = stmt.executeQuery()) {
            int index = 0;
            while (resultSet.next()) {
                PluginColumn pluginColumn = new PluginColumn();
                pluginColumn.setIndex(index);
                pluginColumn.setName(resultSet.getString("COLUMN_NAME"));
                pluginColumn.setType(resultSet.getString("DATA_TYPE"));
                pluginColumns.add(pluginColumn);
                index++;
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return null;
        }

        return pluginColumns;
    }

    /**
     * describe: 通过columneType映射字符串
     * creat_user: felix@thinkingdata.cn
     * creat_date: 2021/1/4
     * creat_time: 3:13 下午
     **/
    public String convertName(int columnType) throws Exception {
        switch (columnType) {

            case Types.CHAR:
                return "CHAR";
            case Types.NCHAR:
                return "NCHAR";
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.LONGVARCHAR:
                return "LONGVARCHAR";
            case Types.NVARCHAR:
                return "NVARCHAR";
            case Types.LONGNVARCHAR:
                return "LONGNVARCHAR";
            case Types.CLOB:
                return "CLOB";
            case Types.NCLOB:
                return "NCLOB";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.NUMERIC:
                return "NUMERIC";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.FLOAT:
                return "FLOAT";
            case Types.REAL:
                return "REAL";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.TIME:
                return "TIME";
            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                return "DATE";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BINARY:
                return "TIMESTAMP";
            case Types.VARBINARY:
                return "VARBINARY";
            case Types.BLOB:
                return "BLOB";
            case Types.LONGVARBINARY:
                return "LONGVARBINARY";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.BIT:
                return "BIT";
            case Types.NULL:
                return "NULL";
            default:
                throw new Exception("生成配置文件时，没有找到匹配类型");
        }
    }
}
