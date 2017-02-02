package com.athena.store;

import com.athena.domain.EventMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zeal on 17-2-2.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/spring*.xml"})
public class EventHandleTest {

    @Test
    public void testHandle() {
        EventHandle eventHandle = new EventHandle();
        EventMessage message = new EventMessage();
        message.setDateTime(new Date());
        message.setEventType("login");
        message.setId(1);
        message.setSource("pc");
        Map<String, String> params = new HashMap<String, String>();
        params.put("type","cUser");
        message.setParam(params);
        eventHandle.handle(message);
    }
}
