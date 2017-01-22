package com.athena.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by zeal on 17-1-22.
 */
@Service
public class IndexDataMappingFactory {

    static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    //是不是传说中的倒排索引，哈哈
    private Map<String, Set<IndexDataMapping>> containsDBTables = new HashMap<>();
    private Map<String, Set<IndexDataMapping>> containsEvents = new HashMap<>();

    private List<IndexDataMapping> mapping = new ArrayList<>();

    @PostConstruct
    public void load() throws URISyntaxException, IOException {

        URL resource = getClass().getResource("/");
        Path path = Paths.get(resource.toURI());
        Files.list(path).filter(p -> p.toString().endsWith(".json")).forEach(jsonFile -> {
            try {
                Optional<String> str = Files.newBufferedReader(jsonFile).lines().reduce((l1, l2) -> l1.concat(l2));
                IndexDataMapping indexDataMapping = gson.fromJson(str.get(), IndexDataMapping.class);
                mapping.add(indexDataMapping);
                indexDataMapping.getTables().forEach((k, v) -> {
                    String key = v.getDestination() + v.getSchemaName() + v.getTableName();
                    if (containsDBTables.containsKey(key)) {
                        containsDBTables.get(key).add(indexDataMapping);
                    } else {
                        HashSet<IndexDataMapping> set = new HashSet<>();
                        set.add(indexDataMapping);
                        containsDBTables.put(key, set);
                    }
                });
                indexDataMapping.getEvents().forEach((k, v) -> {
                    String key = v.getEventType();
                    if (containsEvents.containsKey(key)) {
                        containsEvents.get(key).add(indexDataMapping);
                    } else {
                        HashSet<IndexDataMapping> set = new HashSet<>();
                        set.add(indexDataMapping);
                        containsEvents.put(key, set);
                    }
                });

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public List<IndexDataMapping> getMapping() {
        return mapping;
    }

    /**
     * v.getDestination() + v.getSchemaName() + v.getTableName()
     * @param key
     * @return
     */
    public Set<IndexDataMapping> getDBTables(String key) {
        return containsDBTables.get(key);
    }

    /**
     * eventType
     * @param key
     * @return
     */
    public Set<IndexDataMapping> getEvents(String key) {
        return containsEvents.get(key);
    }
}
