package com.exxeta.jobstore.infinispan.autoconfig;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties("org.quartz")
public class QuartzInfinispanConfigurationProperties {
    private Map<String, String> quartzProperties= new HashMap<String, String>();

    public Map<String, String> getQuartzProperties() {
        return quartzProperties;
    }

    public void setQuartzProperties(Map<String, String> quartzProperties) {
        this.quartzProperties = quartzProperties;
    }

}

