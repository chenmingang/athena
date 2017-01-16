package com.athena.config;

import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Created by zeal on 17-1-14.
 */
@Service
public class CommonConfig {
    private Set<String> destinations;
    private String zookeeperAddress;
    private String esAddress;

    public Set<String> getDestinations() {
        return destinations;
    }

    public void setDestinations(Set<String> destinations) {
        this.destinations = destinations;
    }

    public String getZookeeperAddress() {
        return zookeeperAddress;
    }

    public void setZookeeperAddress(String zookeeperAddress) {
        this.zookeeperAddress = zookeeperAddress;
    }

    public String getEsAddress() {
        return esAddress;
    }

    public void setEsAddress(String esAddress) {
        this.esAddress = esAddress;
    }
}
