package com.athena.config;

import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by zeal on 17-1-22.
 */
public class IndexDataMappingFactoryTest {
    @Test
    public void test() throws IOException, URISyntaxException {
        IndexDataMappingFactory factory = new IndexDataMappingFactory();
        factory.load();
    }
}
