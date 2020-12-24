package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.dataxmeta.PluginColumn;

import java.util.List;

public interface ColumnGenerator {

    List<PluginColumn> getPluginColumns();
}
