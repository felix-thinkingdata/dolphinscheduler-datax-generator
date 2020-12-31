import org.apache.dolphinscheduler.api.dto.dataxmeta.PluginColumn;
import org.apache.dolphinscheduler.api.service.DataxColumnGenerateService;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataxMysqlReaderColumnGenerateServiceImpl extends DataxColumnGenerateService {

    private static final Logger logger = LoggerFactory.getLogger(DataxMysqlReaderColumnGenerateServiceImpl.class);


    @Override
    public List<PluginColumn> getPluginColumns() {

        String jdbcUrl = getPluginParamInfo().get("jdbcUrl");
        String userName = getPluginParamInfo().get("userName");
        String password = getPluginParamInfo().get("password");
        String jdbcDriver = getPluginParamInfo().get("jdbcDriver");

        logger.info("jdbcUrl : {} ,userName : {},password :{} ,jdbcDriver : {}", jdbcUrl, userName, password, jdbcDriver);

        List<PluginColumn> pluginColumns = new ArrayList<>();

        return pluginColumns;
    }
}
