package com.alibaba.otter.shared.common.model.config.data;

import com.alibaba.fastjson.JSON;

import java.util.Map;

/**
 * Created by noah on 2017/12/15.
 */
public class TableRegex {
    private String tableName;
    private Map<String, String> columnRules;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, String> getColumnRules() {
        return columnRules;
    }

    public void setColumnRules(Map<String, String> columnRules) {
        this.columnRules = columnRules;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
