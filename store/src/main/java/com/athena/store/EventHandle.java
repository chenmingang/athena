package com.athena.store;

import com.athena.config.IndexDataMappingFactory;
import com.athena.domain.EventMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by zeal on 17-1-14.
 */
@Service
public class EventHandle implements DataHandle {

    @Autowired
    IndexDataMappingFactory indexDataMappingFactory;

    @Override
    public boolean handle(Object data) {
        if (data instanceof EventMessage) {
            EventMessage eventMessage = (EventMessage) data;
            
        } else {
            throw new IllegalArgumentException("未支持的数据类型存储");
        }
        return false;
    }
}
