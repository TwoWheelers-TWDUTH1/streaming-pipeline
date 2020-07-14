package com.tw;

import com.tw.services.ApiProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.tw.utils.CloudWatchUtil;

@Component
public class ProducerScheduler {

    private CloudWatchUtil cloudwatchUtil = new CloudWatchUtil();

    @Autowired
    private ApiProducer apiProducer;

    @Value("${producer.url}")
    private String url;

    @Scheduled(cron="${producer.cron}")
    public void scheduledProducer() {

        RestTemplate template = new RestTemplate();
        HttpEntity<String> response = template.exchange(url, HttpMethod.GET, HttpEntity.EMPTY, String.class);

        apiProducer.sendMessage(response);
    }

    @Scheduled(fixedRate = 1000 * 60)
    public void scheduledCloudwatch() {
        cloudwatchUtil.sendHeartBeat();
    }
}
