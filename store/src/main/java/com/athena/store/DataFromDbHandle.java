package com.athena.store;

import com.athena.domain.CanalClientObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zeal on 17-1-14.
 */
public class DataFromDbHandle implements DataHandle {

    protected final static Logger logger = LoggerFactory.getLogger(DataFromDbHandle.class);

    @Override
    public boolean handle(Object data) {
        if (data instanceof CanalClientObj) {
            CanalClientObj obj = (CanalClientObj) data;

        } else {
            throw new IllegalArgumentException("未支持的数据类型存储");
        }
        return false;
    }
}
