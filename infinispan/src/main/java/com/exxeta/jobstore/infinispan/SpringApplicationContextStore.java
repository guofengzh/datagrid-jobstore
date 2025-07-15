package com.exxeta.jobstore.infinispan;

import org.springframework.context.ApplicationContext;

public class SpringApplicationContextStore {
    private static ApplicationContext applicationContext;

    public static void setApplicationContext(ApplicationContext applicationContext) {
        SpringApplicationContextStore.applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
