package com.athena.store;

import com.athena.config.IndexDataMapping;
import com.athena.config.IndexDataMappingFactory;
import com.athena.domain.CanalClientObj;
import com.athena.store.es.EsService;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zeal on 17-1-14.
 */
@Service
public class DataFromDbHandle implements DataHandle {

    protected final static Logger logger = LoggerFactory.getLogger(DataFromDbHandle.class);
    protected final static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").create();
    @Autowired
    IndexDataMappingFactory indexDataMappingFactory;
    @Autowired
    EsService esService;

    @Override
    public boolean handle(Object data) {
        if (data instanceof CanalClientObj) {
            CanalClientObj obj = (CanalClientObj) data;
            Set<IndexDataMapping> dbTables = indexDataMappingFactory.getDBTables(obj.getDestination()
                    + obj.getSchemaName() + obj.getTableName());
            if (dbTables != null) {
                dbTables.forEach(index -> {
                    String indexName = index.getIndex();
                    String typeName = index.getType();
                    String primaryKey = index.getPrimaryKey();
                    if (indexName == null || typeName == null || primaryKey == null) {
                        return;
                    }
                    IndexDataMapping.Table table = index.getTables().get(obj.getTableName());
                    if (table == null) {
                        return;
                    }
                    Map<String, String> indexMap = generateData(primaryKey, table, obj);
                    if (!indexMap.isEmpty()) {
                        esService.save(indexName, typeName, gson.toJson(indexMap), indexMap.get(primaryKey));
                    }
                });
            }

        } else {
            throw new IllegalArgumentException("未支持的数据类型存储");
        }
        return false;
    }

    private Map<String, String> generateData(String primaryKey, IndexDataMapping.Table table, CanalClientObj obj) {
        Map<String, String> indexData = new HashMap<>();
        String primaryValue = null;
        try {
            primaryValue = foundPrimaryValue(table, obj);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        if (primaryValue == null) {
            return indexData;
        }
        Sets.SetView<String> difference = Sets.difference(table.getFields().keySet(), obj.getDiffFields());
        try {
            Class<? extends CanalClientObj> aClass = obj.getClass();
            Field primaryKeyField = aClass.getDeclaredField(primaryKey);
            primaryKeyField.setAccessible(true);
            indexData.put(primaryKey, primaryValue);
            difference.forEach(changeKey -> {
                try {
                    Field changeField = aClass.getDeclaredField(changeKey);
                    changeField.setAccessible(true);
                    String changeValue = (String) changeField.get(aClass);
                    switch (table.getFields().get(changeKey)) {
                        case "Date": {
                            changeValue = changeValue.replace(" ", "'T'").concat("Z");
                            break;
                        }
                        default:
                            break;

                    }
                    indexData.put(changeKey, changeValue);
                } catch (NoSuchFieldException e) {
                    logger.error("{}", e);
                } catch (IllegalAccessException e) {
                    logger.error("{}", e);
                }
            });
        } catch (NoSuchFieldException e) {
            logger.error("{}", e);
        } catch (Exception e) {
            logger.error("数据存储失败{}", e);
        }
        return indexData;
    }

    // more things to do
    private String foundPrimaryValue(IndexDataMapping.Table table, CanalClientObj obj) throws IllegalAccessException, NoSuchFieldException {
        String tableType = table.getType();
        if (tableType.equals("parent")) {
            String primaryKey = table.getPrimaryKey();
            Class<? extends CanalClientObj> aClass = obj.getClass();
            Field primaryKeyField = aClass.getDeclaredField(primaryKey);
            primaryKeyField.setAccessible(true);
            String primaryValue = (String) primaryKeyField.get(obj);
            return primaryValue;
        } else if (tableType.equals("child-single") || tableType.equals("child-multi")) {
            String primaryKey = table.getForeignKey();
            Class<? extends CanalClientObj> aClass = obj.getClass();
            Field primaryKeyField = aClass.getDeclaredField(primaryKey);
            primaryKeyField.setAccessible(true);
            String primaryValue = (String) primaryKeyField.get(obj);
            return primaryValue;
        }
        return null;
    }
}
