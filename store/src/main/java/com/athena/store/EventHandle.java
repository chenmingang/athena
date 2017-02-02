package com.athena.store;

import com.athena.config.IndexDataMapping;
import com.athena.config.IndexDataMappingFactory;
import com.athena.domain.EventMessage;
import com.athena.store.es.EsService;
import org.elasticsearch.script.Script;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;

/**
 * Created by zeal on 17-1-14.
 */
@Service
public class EventHandle implements DataHandle {

    @Autowired
    IndexDataMappingFactory indexDataMappingFactory;
    @Autowired
    EsService esService;
    private String mapPath = "events";

    @Override
    public boolean handle(Object data) {
        if (data == null) {
            return false;
        }
        if (data instanceof EventMessage) {
            EventMessage eventMessage = (EventMessage) data;
            String eventType = eventMessage.getEventType();
            if (eventMessage == null) {
                return false;
            }
            Set<IndexDataMapping> mappings = indexDataMappingFactory.getEvents(eventType);
            for (IndexDataMapping indexDataMapping : mappings) {
                Map<String, IndexDataMapping.Event> events = indexDataMapping.getEvents();
                for (Map.Entry<String, IndexDataMapping.Event> kv : events.entrySet()) {
                    String index = indexDataMapping.getIndex();
                    String type = indexDataMapping.getType();
                    IndexDataMapping.Event value = kv.getValue();
                    String valueEventType = value.getEventType();
                    String valueName = value.getName();
                    StringBuilder scriptStr = new StringBuilder("");
                    scriptStr.append("if(!ctx._source.");
                    scriptStr.append(mapPath);
                    scriptStr.append(".contains('");
                    scriptStr.append(valueEventType);
                    scriptStr.append("'))");
                    scriptStr.append("{ctx._source.");
                    scriptStr.append(mapPath);
                    scriptStr.append(".add");

                    Script script = new Script(scriptStr.toString());
                    esService.updateByScript(index, type, String.valueOf(eventMessage.getId()), script);
                }
            }
        } else {
            throw new IllegalArgumentException("未支持的数据类型存储");
        }
        return false;
    }
}
