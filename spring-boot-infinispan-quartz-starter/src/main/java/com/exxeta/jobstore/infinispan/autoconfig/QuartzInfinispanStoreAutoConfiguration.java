package com.exxeta.jobstore.infinispan.autoconfig;

import com.exxeta.jobstore.infinispan.InfinispanJobStore;
import com.exxeta.jobstore.infinispan.SpringApplicationContextStore;
import org.jboss.logging.Logger;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

@Configuration
@EnableConfigurationProperties({QuartzInfinispanConfigurationProperties.class})
public class QuartzInfinispanStoreAutoConfiguration {
    private static final Logger LOGGER = Logger.getLogger(QuartzInfinispanStoreAutoConfiguration.class);

    @Bean
    public JobFactory jobFactory(ApplicationContext applicationContext) {
        AutowiringSpringBeanJobFactory jobFactory = new AutowiringSpringBeanJobFactory();
        jobFactory.setApplicationContext(applicationContext);
        return jobFactory;
    }

    @Bean
    public SchedulerFactoryBean schedulerFactoryBean(
            //DataSource dataSource,
            ApplicationContext applicationContext, JobFactory jobFactory,
            //SchedulerFactory schedulerFactory,
            QuartzInfinispanConfigurationProperties quartzProperties,
            JobsListenerService jobsListenerService) throws IOException {

        SpringApplicationContextStore.setApplicationContext(applicationContext);

        SchedulerFactoryBean factory = new SchedulerFactoryBean();
        //factory.setDataSource(dataSource);
        factory.setJobFactory(jobFactory);
        //factory.setSchedulerFactory(schedulerFactory);
        Properties properties = createProperties(quartzProperties);
        factory.setQuartzProperties(properties);
        factory.setGlobalJobListeners(jobsListenerService);
        // https://medium.com/@rudra.ramesh/use-following-code-in-supervisor-app-while-creating-schedulerfactorybean-object-now-supervisor-fd2f95365350
        // If you need to disable launching of jobs on supervisor use this:
        //factory.setAutoStartup(false);
        return factory;
    }

    /*
    @Bean
    public SchedulerFactory scheduler(ApplicationContext applicationContext,
                                      QuartzInfinispanConfigurationProperties quartzProperties) throws SchedulerException {
        // setup application context
        SpringApplicationContextStore.setApplicationContext(applicationContext);
        Properties properties = createProperties(quartzProperties);
        SchedulerFactory schedulerFactory = new org.quartz.impl.StdSchedulerFactory(properties);
        //Scheduler scheduler = schedulerFactory.getScheduler();
        //LOGGER.info("-------------Starting Scheduler---------------");
        //scheduler.start();
        //return scheduler;
        LOGGER.info("Created schedulerFactory");
        return schedulerFactory;
    }*/

    private Properties createProperties( QuartzInfinispanConfigurationProperties quartzProperties) {
        Properties properties = new Properties();
        Map<String, String> quartzConfigMap = quartzProperties.getQuartzProperties();
        for ( String key : quartzProperties.getQuartzProperties().keySet() ) {
            properties.setProperty(key, quartzConfigMap.get(key));
        }

        // set the store class
        properties.setProperty("org.quartz.jobStore.class", InfinispanJobStore.class.getCanonicalName());
        if (!properties.containsKey("org.quartz.jobStore.misfireThreshold"))
            properties.setProperty("org.quartz.jobStore.misfireThreshold", "60000");
        if (!properties.containsKey("org.quartz.threadPool.class"))
            properties.setProperty("org.quartz.threadPool.class", SimpleThreadPool.class.getCanonicalName());
        if (!properties.containsKey("org.quartz.threadPool.threadCount"))
            properties.setProperty("org.quartz.threadPool.threadCount", "5");
        if (!properties.containsKey("org.quartz.threadPool.threadPriority"))
            properties.setProperty("org.quartz.threadPool.threadPriority", "5");
        if (!properties.containsKey("org.quartz.scheduler.instanceId"))
            properties.setProperty("org.quartz.scheduler.instanceId", "AUTO");

        // set infinispan cache name to use
        properties.setProperty("org.quartz.jobStore.infinispanJobStoreCacheJobsByKey", "jobs");
        properties.setProperty("org.quartz.jobStore.infinispanJobStoreCacheTriggersByKey", "triggers");
        properties.setProperty("org.quartz.jobStore.infinispanJobStoreCacheCalendarsByName", "calendars");
        properties.setProperty("org.quartz.jobStore.infinispanJobStoreCacheMetaData", "meta");

        return properties;
    }
}
