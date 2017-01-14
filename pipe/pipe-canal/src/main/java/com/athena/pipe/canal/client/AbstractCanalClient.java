package com.athena.pipe.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.athena.domain.CanalClientObj;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 测试基类
 *
 * @author jianghang 2013-4-15 下午04:17:12
 * @version 1.0.4
 */
public abstract class AbstractCanalClient {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected volatile boolean running = false;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };
    protected Thread thread = null;
    protected CanalConnector connector;

    public AbstractCanalClient(){}

    protected void start(final String destination) {
        thread = new Thread(new Runnable() {

            public void run() {
                process(destination);
                logger.info("[{} canal客户端]启动成功", destination);
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
//    public void startCanal(String destination) {
//        Properties pro = new Properties();
//        try {
//            pro.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        String zookeeperAddress = pro.getProperty("canalZookeeperAddress");
//        CanalConnector connector = CanalConnectors.newClusterConnector(zookeeperAddress, destination, "", "");
//        this.setConnector(connector);
//        this.start(destination);
//    }
    protected void process(String destination) {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                connector.connect();
                logger.info("[canal客户端]连接server成功, destination={}", destination);
                connector.subscribe();
                logger.info("[canal客户端]订阅到数据, destination={}", destination);
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {

                    } else {
                        printEntry(message.getEntries());
                    }
                    connector.ack(batchId); // 提交确认
                }
            } catch (Exception e) {
                logger.error("process error! destination={}", e, destination);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } finally {
                connector.disconnect();
                logger.info("[canal客户端]断开连接, destination={}", destination);
            }
        }
    }

    protected void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                CanalEntry.EventType eventType = rowChage.getEventType();
                if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.DELETE) {
                        printRow("DELETE",entry.getHeader().getSchemaName(),entry.getHeader().getTableName(),rowData);
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printRow("INSERT",entry.getHeader().getSchemaName(),entry.getHeader().getTableName(),rowData);
                    } else {
                        printRow("UPDATE",entry.getHeader().getSchemaName(),entry.getHeader().getTableName(),rowData);
                    }
                }
            }
        }
    }
    protected CanalClientObj printRow(String eventType, String schemaName, String tableName, CanalEntry.RowData rowData, String destination) {

        CanalClientObj obj = new CanalClientObj();
        obj.setDestination(destination);
        obj.setSchemaName(schemaName);
        obj.setEventType(eventType);
        obj.setTableName(tableName);
        Set<String> diffFields = new HashSet<>();
        Map<String,String> content = new HashMap<>();
        Map<String,String> beforeContent = new HashMap<>();
        obj.setContent(content);
        obj.setBeforeContent(beforeContent);
        obj.setDiffFields(diffFields);

        for (CanalEntry.Column c : rowData.getAfterColumnsList()) {
            if(!c.getValue().equals("")){
                if(c.getUpdated()){
                    diffFields.add(c.getName());
                }
                content.put(c.getName(),c.getValue());
            }
        }

        for (CanalEntry.Column c : rowData.getBeforeColumnsList()) {
            if(!c.getValue().equals("")){
                if(c.getUpdated()){
                    diffFields.add(c.getName());
                }
                beforeContent.put(c.getName(),c.getValue());
            }
        }

        return obj;
    }
    protected abstract void printRow(String eventType,String schemaName,String tableName, CanalEntry.RowData rowData);
    public abstract void startCanal();

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

}
