package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

import java.util.Arrays;
import java.util.List;

/**
 * Created by noah on 2017/12/13.
 */
public class UnitProcessor extends AbstractEventProcessor {

    protected Integer unit_id = null;

    private List tables = Arrays.asList("t1", "t2");

    public boolean process(EventData eventData) {
        if (tables.indexOf(eventData.getTableName()) != -1) {
            final EventColumn eventColumn = getColumn(eventData, "unit_id");
            if (eventColumn != null && eventColumn.getColumnValue() != null) {
                if (eventColumn.getColumnValue().equals(unit_id)) {
                    return true;
                }
            }
        }
        return false;
    }

    protected Object genIdByFName() {
        String superFileName = this.getClass().getSuperclass().getName();
        return this.getClass().getName().substring(superFileName.length());
    }
}
