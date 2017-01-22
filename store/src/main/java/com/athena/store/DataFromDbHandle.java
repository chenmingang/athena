package com.athena.store;

import com.athena.config.IndexDataMappingFactory;
import com.athena.domain.CanalClientObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by zeal on 17-1-14.
 */
@Service
public class DataFromDbHandle implements DataHandle {

    protected final static Logger logger = LoggerFactory.getLogger(DataFromDbHandle.class);
    @Autowired
    IndexDataMappingFactory indexDataMappingFactory;

    @Override
    public boolean handle(Object data) {
        if (data instanceof CanalClientObj) {
            CanalClientObj obj = (CanalClientObj) data;
            indexDataMappingFactory.getMapping().forEach(indexDataMapping -> {

            });

        } else {
            throw new IllegalArgumentException("未支持的数据类型存储");
        }
        return false;
    }
}
