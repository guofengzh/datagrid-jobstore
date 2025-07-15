package com.exxeta.jobstore.infinispan.demo;

import com.exxeta.jobstore.infinispan.autoconfig.JobsListenerService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(JobsListenerService.class)
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
