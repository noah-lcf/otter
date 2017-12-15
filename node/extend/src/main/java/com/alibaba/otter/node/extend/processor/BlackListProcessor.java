package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by noah on 2017/12/7.
 */
public class BlackListProcessor extends AbstractEventProcessor {

    Map<String, List> blackList = new HashMap<String, List>() {{
        put("unit_id", Arrays.asList(1, 2, 3, 4, 5, 6, 7));
    }};


    public boolean process(EventData eventData) {
        for (Map.Entry<String, List> entry : blackList.entrySet()) {
            final EventColumn eventColumn = getColumn(eventData, entry.getKey());
            if (eventColumn != null && eventColumn.getColumnValue() != null) {
                boolean inBlackList = entry.getValue().stream().anyMatch(p -> String.valueOf(p).equals(eventColumn.getColumnValue()));
                if (inBlackList) {
                    return false;
                }
            }
        }
        return true;
    }
}
