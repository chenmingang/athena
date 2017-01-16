package com.athena.store;

import com.athena.domain.EventMessage;
import org.springframework.stereotype.Service;

/**
 * Created by zeal on 17-1-14.
 */
@Service
public class EventHandle implements DataHandle {
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
