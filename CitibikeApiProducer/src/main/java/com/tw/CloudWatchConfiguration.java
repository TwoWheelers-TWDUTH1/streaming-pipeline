package com.tw;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CloudWatchConfiguration {

    @Bean
    public static AmazonCloudWatch amazonCloudWatch() {
        return AmazonCloudWatchClientBuilder.defaultClient();
    }

}
