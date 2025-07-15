package com.exxeta.jobstore.infinispan.demo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.concurrent.TimeUnit;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class TestControllerTest {
    private static final Logger logger = LoggerFactory.getLogger(TestControllerTest.class);

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void tesDeleteFileStore() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.post("/jobs")
                        .param("jobs", "5")
                )
                .andExpect(status().isOk())
                .andReturn();

        Thread.sleep(TimeUnit.MINUTES.toMillis(1));

        MvcResult get = this.mockMvc.perform(MockMvcRequestBuilders.get("/jobs"))
                .andExpect(status().isOk())
                .andReturn();

        System.out.println(get.getResponse().getContentAsString());
    }
}
