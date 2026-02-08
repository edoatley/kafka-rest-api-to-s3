package com.example.kafka.s3.sink.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

	@Bean
	public Clock clock() {
		return Clock.systemUTC();
	}

	@Bean
	public org.apache.hadoop.conf.Configuration hadoopConfiguration() {
		return new org.apache.hadoop.conf.Configuration();
	}
}
