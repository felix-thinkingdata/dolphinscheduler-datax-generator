package org.apache.dolphinscheduler.api.service;

import java.util.HashMap;

public abstract class DataxColumnGenerateService  implements ColumnGenerator{


    HashMap<String, String> pluginParamInfo;

    String pluginId;


    public HashMap<String, String> getPluginParamInfo() {
        return pluginParamInfo;
    }

    public void setPluginParamInfo(HashMap<String, String> pluginParamInfo) {
        this.pluginParamInfo = pluginParamInfo;
    }

    public String getPluginId() {
        return pluginId;
    }

    public void setPluginId(String pluginId) {
        this.pluginId = pluginId;
    }


}
