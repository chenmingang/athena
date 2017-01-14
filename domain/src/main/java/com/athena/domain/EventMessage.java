package com.athena.domain;

import java.util.Date;
import java.util.Map;

/**
 * Created by zeal on 17-1-14.
 */
public class EventMessage {

    /**消息类型*/
    private String eventType;
    /**关联ID*/
    private Integer id;
    /**消息来源 */
    private String source;

    private Map<String,String> param;
    /**日期时间*/
    private Date dateTime;

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Map<String, String> getParam() {
        return param;
    }

    public void setParam(Map<String, String> param) {
        this.param = param;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }
}
