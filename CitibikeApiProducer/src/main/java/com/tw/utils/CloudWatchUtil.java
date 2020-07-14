package com.tw.utils;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.Set;
import java.util.HashSet;

public class CloudWatchUtil {

    @Value("${spring.profiles.active}")
    private String appName;

    private static Logger logger = LoggerFactory.getLogger(CloudWatchUtil.class);

    private AmazonCloudWatch cw;

    public CloudWatchUtil(AmazonCloudWatch cw) {
        this.cw = cw;
    }

    public PutMetricDataResult sendHeartBeat() {
        return this.putMetric("is_app_running", 1.0, StandardUnit.None);
    }

    public PutMetricDataResult putMetric(String metricName, Double value, StandardUnit unit) {

        Dimension dimensionAppName = new Dimension()
                .withName("ApplicationName")
                .withValue(this.appName);

        Dimension dimensionInstanceId = new Dimension()
                .withName("InstanceId")
                .withValue("InstanceId");

        Set<Dimension> dimensionSet = new HashSet<>();
        dimensionSet.add(dimensionAppName);
        dimensionSet.add(dimensionInstanceId);

        MetricDatum datum = new MetricDatum()
                .withMetricName(metricName)
                .withUnit(unit)
                .withValue(value)
                .withDimensions(dimensionSet);

        PutMetricDataRequest request = new PutMetricDataRequest()
                .withNamespace("APP/Monitoring")
                .withMetricData(datum);

        PutMetricDataResult response = cw.putMetricData(request);

        if (response.getSdkHttpMetadata().getHttpStatusCode() != 200) {
            logger.warn("Failed pushing CloudWatch Metric with RequestId: " + response.getSdkResponseMetadata().getRequestId());
            logger.debug("Response Status code: " + response.getSdkHttpMetadata().getHttpStatusCode());
        }

        return response;

    }
}
