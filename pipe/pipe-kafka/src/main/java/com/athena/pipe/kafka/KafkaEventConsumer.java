package com.athena.pipe.kafka;

import com.athena.store.EventHandle;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import sun.plugin2.message.EventMessage;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zeal on 17-1-14.
 */
@Service
public class KafkaEventConsumer {

    protected final static Logger logger = LoggerFactory.getLogger(KafkaEventConsumer.class);
    static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    private RateLimiter limiter = RateLimiter.create(50);
    final String topicStr = "ATHENA_EVENT_TOPIC";
    String zookeeper = "localhost:2181";
    String groupId = "ATHENA_EVENT_TOPIC_GROUP";

    private EventHandle eventHandle;

    @PostConstruct
    public void start() {

        final Map<String, Integer> topic = new HashMap<>();
        topic.put(topicStr, new Integer(1));
        new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("消费kafka消息{}", topicStr);
                KafkaEventConsumer.this.run(topic, topicStr);
            }
        }).start();
    }

    private void run(Map<String, Integer> topic, String k) {
        final ConsumerConnector consumer = createConsumer();
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topic);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(k);
        for (final KafkaStream stream : kafkaStreams) {
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            int count = 0;
            while (iterator.hasNext()) {
                String msg = new String(iterator.next().message());
                try {
                    limiter.acquire();
                    consume(k, msg);
                    count++;
                    if (count >= 100) {
                        consumer.commitOffsets();
                        count = 0;
                    }
                } catch (Exception e) {
                    logger.error("消费异常 - {} - {}", msg, e);
                }
            }
        }
    }

    /**
     * 消费逻辑
     *
     * @param topic
     * @return
     */
    public boolean consume(String topic, String msg) {
        try {
            EventMessage eventMessage = gson.fromJson(msg, EventMessage.class);
            eventHandle.handle(eventMessage);
        } catch (Exception e) {
            logger.error("未支持的事件消息类型{}",e);
        }
        return true;
    }

    private ConsumerConnector createConsumer() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "5000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.commit.enable", "false");
        props.put("rebalance.max.retries", "5");
        props.put("rebalance.backoff.ms", "1200");
        return kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }
}
