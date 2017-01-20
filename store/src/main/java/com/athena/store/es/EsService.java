package com.athena.store.es;

import com.athena.domain.EventMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by zeal on 16-4-11.
 */
@Service
public class EsService {

    private static Logger logger = LoggerFactory.getLogger(EsService.class);
    private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").create();

    @Resource
    private ESClientFactory esClientFactory;

    protected static Gson getGson() {
        return gson;
    }

    protected Client getClient() {
        return esClientFactory.getClient();
    }

    /**
     * 创建索引
     *
     * @param indexName
     * @param typeName
     * @param obj
     * @param primaryValue
     * @return
     */
    public boolean save(String indexName, String typeName, Object obj, String primaryValue) {
        return save(indexName, typeName, gson.toJson(obj), primaryValue);
    }
    /**
     * 创建索引
     *
     * @param indexName
     * @param typeName
     * @param map
     * @param primaryValue
     * @return
     */
    public boolean save(String indexName, String typeName, Map<String, String> map, String primaryValue) {
        return save(indexName, typeName, gson.toJson(map), primaryValue);
    }
    /**
     * 创建索引
     *
     * @param indexName
     * @param typeName
     * @param json
     * @param primaryValue
     * @return
     */
    public boolean save(String indexName, String typeName, String json, String primaryValue) {
        IndexRequest indexRequest = new IndexRequest(indexName, typeName, primaryValue).source(json);
        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, primaryValue)
                .doc(json)
                .upsert(indexRequest)
                .retryOnConflict(3);
        try {
            getClient().update(updateRequest).get();
        } catch (Exception e) {
            logger.error("保存失败！本次操作数据丢失{}{}", json, e);
        }
        return false;
    }

    /**
     * @param indexName
     * @param typeName
     * @param objs
     * @param primaryKey
     * @return
     */
    public boolean save(String indexName, String typeName, List<Object> objs, String primaryKey) {
        if (objs == null || objs.isEmpty()) {
            return false;
        }
        BulkRequestBuilder bulkRequestBuilder = getClient().prepareBulk();
        for (Object obj : objs) {
            try {
                Field primaryField = obj.getClass().getDeclaredField(primaryKey);
                primaryField.setAccessible(true);
                String primaryValue = String.valueOf(primaryField.get(obj));

                IndexRequest indexRequest = new IndexRequest(indexName, typeName, primaryValue).source(gson.toJson(obj));
                UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, primaryValue)
                        .doc(gson.toJson(obj)).upsert(indexRequest).retryOnConflict(3);
                bulkRequestBuilder.add(updateRequest);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        BulkResponse bulkItemResponses = bulkRequestBuilder.execute().actionGet();
        return true;
    }

    /**
     * 移除一个字段，让它像不曾出现过一样
     *
     * @param indexName
     * @param typeName
     * @param primaryValue
     * @param field
     * @return
     */
    public boolean removeField(String indexName, String typeName, String primaryValue, String field) {

        //拼参数
        List<String> fieldList = new ArrayList<>();
        fieldList.add(field);

        //执行
        boolean result = removeFields(indexName, typeName, primaryValue, fieldList);

        return result;
    }

    /**
     * 移除字段List，让它像不曾出现过一样
     *
     * @param indexName
     * @param typeName
     * @param primaryValue
     * @param fields
     * @return
     */
    public boolean removeFields(String indexName, String typeName, String primaryValue, List<String> fields) {
        StringBuilder builder = new StringBuilder("");
        for (String f : fields) {
            builder.append("ctx._source.remove(\"");
            builder.append(f);
            builder.append("\")");
            builder.append(";");
        }
        Script script = new Script(builder.toString());
        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, primaryValue)
                .script(script).retryOnConflict(3);
        try {
            getClient().update(updateRequest).get();
        } catch (Exception e) {
            logger.info("更新失败！本次操作数据丢失{}", builder.toString());
        }
        return true;
    }

    /**
     * @param indexName
     * @param typeName
     * @param primaryValue
     * @param script
     * @return
     */
    public boolean updateByScript(String indexName, String typeName, String primaryValue, Script script) {
        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, primaryValue)
                .script(script).retryOnConflict(3);
        try {
            getClient().update(updateRequest).get();
        } catch (Exception e) {
            logger.info("重试失败！本次操作数据丢失{}", script.toString());
        }
        return true;
    }

    /**
     * 删除操作
     *
     * @param indexName
     * @param typeName
     * @param primaryValue
     * @return
     */
    public boolean delete(String indexName, String typeName, String primaryValue) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, primaryValue);
        try {
            getClient().delete(deleteRequest).get();
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return false;
    }

    public <T> List<T> query(String indexName, String[] typeName, QueryBuilder queryBuilder, SortBuilder sortBuilder,
                             Class<T> tClass, Integer limitStart, Integer limitEnd) {
        if (indexName == null || typeName == null || queryBuilder == null) {
            throw new IllegalArgumentException("参数不正确");
        }
        if (limitStart == null) {
            limitStart = 0;
        }
        if (limitEnd == null) {
            limitEnd = 100;
        }
        List<T> result = new ArrayList();
        SearchRequestBuilder builder = getClient().prepareSearch(indexName)
                .setTypes(typeName)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)                 // Query
                .setFrom(limitStart)
                .setSize(limitEnd)
                .setExplain(true);
        if (sortBuilder != null) {
            builder.addSort(sortBuilder);
        }
        SearchResponse response = builder.execute().actionGet();
        for (SearchHit hit : response.getHits().getHits()) {
            String fields = hit.getSourceAsString();
            T t = null;
            try {
                t = gson.fromJson(fields, tClass);
            } catch (Exception e) {
                Gson gson1 = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create();
                t = gson1.fromJson(fields, tClass);
            }
            result.add(t);
        }
        return result;
    }

    public <T> List<T> query(String indexName, String typeName, QueryBuilder queryBuilder, SortBuilder sortBuilder,
                             Class<T> tClass, Integer limitStart, Integer limitEnd) {
        return query(indexName, new String[]{typeName}, queryBuilder, sortBuilder, tClass, limitStart, limitEnd);
    }


    public long count(String indexName, String[] typeName, QueryBuilder queryBuilder) {
        SearchResponse searchResponse = getClient().prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(queryBuilder)
                .execute().actionGet();

        SearchHits hits = searchResponse.getHits();
        long count = hits.getTotalHits();
        return count;
    }

    public long count(String indexName, String typeName, QueryBuilder queryBuilder) {
        return count(indexName, new String[]{typeName}, queryBuilder);
    }

    public <T> T get(String indexName, String typeName, String primaryValue, Class<T> tClass) {
        GetResponse response = getClient().prepareGet(indexName, typeName, primaryValue).get();
        String sourceAsString = response.getSourceAsString();
        if (sourceAsString == null) {
            return null;
        } else {
            return gson.fromJson(sourceAsString, tClass);
        }
    }

    public boolean save(EventMessage message) {

        return false;
    }

}
