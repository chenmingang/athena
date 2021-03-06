package com.athena.pipe.canal.client.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.athena.domain.CanalClientObj;
import com.athena.pipe.canal.client.AbstractCanalClient;
import com.athena.store.DataFromDbHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Set;

/**
 * Created by zeal on 16-1-12.
 */
@Service
public class CanalClinetImpl {
    protected final static Logger logger = LoggerFactory.getLogger(CanalClinetImpl.class);

    @Value("${destinations}")
    private Set<String> destinations;
    @Value("${zookeeperAddress}")
    private String zookeeperAddress;
    @Autowired
    private DataFromDbHandle dataFromDbHandle;

    @PostConstruct
    public void startCanal() {
        if (destinations != null) {
            for (final String destination : destinations) {
                new AbstractCanalClient() {
                    @Override
                    protected void printRow(String eventType, String schemaName, String tableName, CanalEntry.RowData rowData) {
                        CanalClientObj canalClientObj = this.printRow(eventType, schemaName, tableName, rowData, destination);
                        handle(canalClientObj);
                    }

                    @Override
                    public void startCanal() {
                        CanalConnector connector = CanalConnectors.newClusterConnector(zookeeperAddress, destination, "", "");
                        this.setConnector(connector);
                        this.start(destination);
                    }
                }.startCanal();
            }
        }
    }


    protected void handle(CanalClientObj canalClientObj) {
        //do something
        dataFromDbHandle.handle(canalClientObj);
    }

    public void setDestinations(Set<String> destinations) {
        this.destinations = destinations;
    }

    public void setZookeeperAddress(String zookeeperAddress) {
        this.zookeeperAddress = zookeeperAddress;
    }

    public void setDataFromDbHandle(DataFromDbHandle dataFromDbHandle) {
        this.dataFromDbHandle = dataFromDbHandle;
    }
}
