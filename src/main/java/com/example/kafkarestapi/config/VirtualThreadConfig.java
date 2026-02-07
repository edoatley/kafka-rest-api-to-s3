package com.example.kafkarestapi.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;

@Configuration
public class VirtualThreadConfig {

    @Bean
    public TaskExecutor applicationTaskExecutor() {
        return new VirtualThreadTaskExecutor("vt-exec-");
    }
}
