package com.athena.domain;

import java.util.Map;
import java.util.Set;

/**
 * Created by zeal on 16-1-11.
 */
public class CanalClientObj {

    private String destination;
    private String eventType;
    private String schemaName;
    private String tableName;
    private Map<String,String> content;
    private Map<String,String> beforeContent;
    private Set<String> diffFields;

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, String> getContent() {
        return content;
    }

    public void setContent(Map<String, String> String) {
        this.content = content;
    }

    public Map<String, String> getBeforeContent() {
        return beforeContent;
    }

    public void setBeforeContent(Map<String, String> beforeContent) {
        this.beforeContent = beforeContent;
    }

    public Set<String> getDiffFields() {
        return diffFields;
    }

    public void setDiffFields(Set<String> diffFields) {
        this.diffFields = diffFields;
    }
}
