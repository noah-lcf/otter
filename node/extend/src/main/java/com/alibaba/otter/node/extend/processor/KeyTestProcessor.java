package com.alibaba.otter.node.extend.processor;

/**
 * Created by noah on 2017/12/7.
 */

import com.alibaba.otter.shared.etl.extend.processor.support.DataSourceFetcher;
import com.alibaba.otter.shared.etl.extend.processor.support.DataSourceFetcherAware;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

public class KeyTestProcessor extends AbstractEventProcessor implements DataSourceFetcherAware {

    public boolean process(EventData eventData) {
        for (EventColumn key : eventData.getKeys()) {
            if ((Integer.valueOf(key.getColumnValue()) > 10000)) {
                return false;
            }
        }
        return true;
    }


    @Override
    public void setDataSourceFetcher(DataSourceFetcher dataSourceFetcher) {

    }
}
