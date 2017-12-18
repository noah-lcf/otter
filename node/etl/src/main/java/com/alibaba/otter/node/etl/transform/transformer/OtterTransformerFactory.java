/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.otter.node.etl.transform.transformer;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.node.common.config.ConfigClientService;
import com.alibaba.otter.node.etl.OtterConstants;
import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.node.etl.transform.exception.TransformException;
import com.alibaba.otter.shared.common.model.config.ConfigHelper;
import com.alibaba.otter.shared.common.model.config.data.DataMedia;
import com.alibaba.otter.shared.common.model.config.data.DataMediaPair;
import com.alibaba.otter.shared.common.model.config.data.TableRegex;
import com.alibaba.otter.shared.common.model.config.data.db.DbDataMedia;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import com.alibaba.otter.shared.common.utils.RegexUtils;
import com.alibaba.otter.shared.common.utils.extension.ExtensionFactory;
import com.alibaba.otter.shared.etl.extend.processor.EventProcessor;
import com.alibaba.otter.shared.etl.extend.processor.support.DataSourceFetcher;
import com.alibaba.otter.shared.etl.extend.processor.support.DataSourceFetcherAware;
import com.alibaba.otter.shared.etl.model.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据对象转化工厂
 *
 * @author jianghang 2011-10-27 下午06:29:02
 * @version 4.0.0
 */
public class OtterTransformerFactory {

    private ConfigClientService configClientService;
    private RowDataTransformer rowDataTransformer;
    private FileDataTransformer fileDataTransformer;
    private ExtensionFactory extensionFactory;
    private DataSourceService dataSourceService;

    /**
     * 将一种源数据进行转化，最后得到的结果会根据DataMediaPair中定义的目标对象生成不同的数据对象 <br/>
     * <p>
     * <pre>
     * 返回对象格式：Map
     * key : Class对象，代表生成的目标数据对象
     * value : 每种目标数据对象的集合数据
     * </pre>
     */
    public Map<Class, BatchObject> transform(RowBatch rowBatch) {
        final Identity identity = translateIdentity(rowBatch.getIdentity());
        Map<Class, BatchObject> result = new HashMap<Class, BatchObject>();
        // 初始化默认值
        result.put(EventData.class, initBatchObject(identity, EventData.class));

        for (EventData eventData : rowBatch.getDatas()) {
            // 处理eventData
            Long tableId = eventData.getTableId();
            final Pipeline pipeline = configClientService.findPipeline(identity.getPipelineId());
            // 针对每个同步数据，可能会存在多路复制的情况
            List<DataMediaPair> dataMediaPairs = ConfigHelper.findDataMediaPairByMediaId(pipeline, tableId);
            for (DataMediaPair pair : dataMediaPairs) {
                if (!pair.getSource().getId().equals(tableId)) { // 过滤tableID不为源的同步
                    continue;
                }

                //add by noah 使用设置的pair正则匹配
                if (StringUtils.isNotBlank(pair.getFieldMatchRegex())) {
                    List<TableRegex> regexList = JSON.parseArray(pair.getFieldMatchRegex(), TableRegex.class);
                    boolean allMatch = true;
                    for (TableRegex tableRegex : regexList) {
                        if (!allMatch) {
                            break;
                        }
                        if (tableRegex.getTableName() != null && tableRegex.getTableName().equals(eventData.getTableName())) {
                            Map<String, String> rules = tableRegex.getColumnRules();
                            for (EventColumn column : eventData.getColumns()) {
                                String columnRule = rules.get(column.getColumnName());
                                if (columnRule != null) {
                                    boolean match = StringUtils.isNotBlank(RegexUtils.findFirst(column.getColumnValue(), columnRule));
                                    if (!match) {
                                        allMatch = false;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (!allMatch) {
                        continue;
                    }
                }

                //add by noah  从com.alibaba.otter.node.etl.extract.extractor.ProcessorExtractor 改放在这里进行过滤 解决一个table id对应多个pair时全部过滤的问题
                if (pair.isExistFilter()) {
                    final EventProcessor eventProcessor = extensionFactory.getExtension(EventProcessor.class,
                            pair.getFilterData());
                    if (eventProcessor instanceof DataSourceFetcherAware) {
                        ((DataSourceFetcherAware) eventProcessor).setDataSourceFetcher(new DataSourceFetcher() {

                            @Override
                            public DataSource fetch(Long tableId) {
                                DataMedia dataMedia = ConfigHelper.findDataMedia(pipeline, tableId);
                                return dataSourceService.getDataSource(pipeline.getId(), dataMedia.getSource());
                            }
                        });

                        MDC.put(OtterConstants.splitPipelineLogFileKey, String.valueOf(pipeline.getId()));
                        boolean process = eventProcessor.process(eventData);
                        if (!process) {
                            continue;
                        }
                    } else {
                        boolean process = eventProcessor.process(eventData);
                        if (!process) {
                            continue;
                        }
                    }
                }
                OtterTransformer translate = lookup(pair.getSource(), pair.getTarget());
                // 进行转化
                Object item = translate.transform(eventData, new OtterTransformerContext(identity, pair, pipeline));

                if (item == null) {
                    continue;
                }
                // 合并结果
                merge(identity, result, item);
            }

        }

        return result;
    }


    protected Pipeline getPipeline(Long pipelineId) {
        return configClientService.findPipeline(pipelineId);
    }

    /**
     * 转化FileBatch对象
     */
    public Map<Class, BatchObject> transform(FileBatch fileBatch) {
        final Identity identity = translateIdentity(fileBatch.getIdentity());
        List<FileData> fileDatas = fileBatch.getFiles();
        Map<Class, BatchObject> result = new HashMap<Class, BatchObject>();
        // 初始化默认值
        result.put(FileData.class, initBatchObject(identity, FileData.class));

        for (FileData fileData : fileDatas) {
            // 进行转化
            Long tableId = fileData.getTableId();
            Pipeline pipeline = configClientService.findPipeline(identity.getPipelineId());
            // 针对每个同步数据，可能会存在多路复制的情况
            List<DataMediaPair> dataMediaPairs = ConfigHelper.findDataMediaPairByMediaId(pipeline, tableId);
            for (DataMediaPair pair : dataMediaPairs) {
                if (!pair.getSource().getId().equals(tableId)) { // 过滤tableID不为源的同步
                    continue;
                }

                Object item = fileDataTransformer.transform(fileData, new OtterTransformerContext(identity, pair,
                        pipeline));
                if (item == null) {
                    continue;
                }
                // 合并结果
                merge(identity, result, item);
            }

        }

        return result;
    }

    // =============================== helper method
    // ============================

    // 将生成的item对象合并到结果对象中
    private synchronized void merge(Identity identity, Map<Class, BatchObject> data, Object item) {
        Class clazz = item.getClass();
        BatchObject batchObject = data.get(clazz);
        // 初始化一下对象
        if (batchObject == null) {
            batchObject = initBatchObject(identity, clazz);
            data.put(clazz, batchObject);
        }

        // 进行merge处理
        if (batchObject instanceof RowBatch) {
            ((RowBatch) batchObject).merge((EventData) item);
        } else if (batchObject instanceof FileBatch) {
            ((FileBatch) batchObject).getFiles().add((FileData) item);
        } else {
            throw new TransformException("no support Data[" + clazz.getName() + "]");
        }
    }

    // 根据对应的类型初始化batchObject对象
    private BatchObject initBatchObject(Identity identity, Class clazz) {
        if (EventData.class.equals(clazz)) {
            RowBatch rowbatch = new RowBatch();
            rowbatch.setIdentity(identity);
            return rowbatch;
        } else if (FileData.class.equals(clazz)) {
            FileBatch fileBatch = new FileBatch();
            fileBatch.setIdentity(identity);
            return fileBatch;
        } else {
            throw new TransformException("no support Data[" + clazz.getName() + "]");
        }
    }

    // 查找对应的tranlate转化对象
    private OtterTransformer lookup(DataMedia sourceDataMedia, DataMedia targetDataMedia) {
        if (sourceDataMedia instanceof DbDataMedia && targetDataMedia instanceof DbDataMedia) {
            return rowDataTransformer;
        }

        throw new TransformException("no support translate for source " + sourceDataMedia.toString() + " to target "
                + targetDataMedia);
    }

    private Identity translateIdentity(Identity identity) {
        Identity result = new Identity();
        result.setChannelId(identity.getChannelId());
        result.setPipelineId(identity.getPipelineId());
        result.setProcessId(identity.getProcessId());
        return result;
    }

    // ==================== setter / getter ==================

    public void setConfigClientService(ConfigClientService configClientService) {
        this.configClientService = configClientService;
    }

    public void setRowDataTransformer(RowDataTransformer rowDataTransformer) {
        this.rowDataTransformer = rowDataTransformer;
    }

    public void setFileDataTransformer(FileDataTransformer fileDataTransformer) {
        this.fileDataTransformer = fileDataTransformer;
    }

    public void setDataSourceService(DataSourceService dataSourceService) {
        this.dataSourceService = dataSourceService;
    }

    public void setExtensionFactory(ExtensionFactory extensionFactory) {
        this.extensionFactory = extensionFactory;
    }


    public static void main(String[] args) {
        List<TableRegex> tableRegexes = JSON.parseArray("[{\"tableName\":\"t1\",\"columnRules\":[{\"name\":\"a|b|c\"}]},{\"tableName\":\"t2\",\"columnRules\":[{\"name\":\"e|f|g\"}]}]", TableRegex.class);
    }
}
