package com.athena.store.es;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by zeal on 2016/10/27.
 */
@Component
public class ESClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(ESClientFactory.class);

    @Value("${es.address}")
    protected String esAddress;

    @Value("${es.cluster.name}")
    protected String esClusterName;

    private Client client;

    @PostConstruct
    protected void init() {
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", esClusterName)
                    .build();
            TransportClient transportClient = new PreBuiltTransportClient(settings);

            if (esAddress != null) {
                String[] adds = esAddress.split(",");
                for (String add : adds) {
                    String[] hostport = add.split(":");
                    transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostport
                            [0]), Integer.parseInt(hostport[1])));
                }
            }
            client = transportClient;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public Client getClient() {
        return client;
    }
}
