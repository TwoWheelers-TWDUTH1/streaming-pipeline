package com.tw.utils;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

import com.amazonaws.util.EC2MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.Optional;
import java.util.Set;
import java.util.HashSet;

public class CloudWatchUtil {

    @Value("${application.name}")
    private String applicationName;

    private static Logger logger = LoggerFactory.getLogger(CloudWatchUtil.class);

    private AmazonCloudWatch cw;

    public CloudWatchUtil(AmazonCloudWatch cw) {
        this.cw = cw;
    }

    public String getInstanceIdWrapper(){
        return EC2MetadataUtils.getInstanceId();
    }

    public PutMetricDataResult sendHeartBeat() {
        return this.putMetric("is_app_running", 1.0, StandardUnit.None);
    }

    public PutMetricDataResult putMetric(String metricName, Double value, StandardUnit unit) {

        String appNameWithDefault = Optional.ofNullable(applicationName).orElse("IngesterApp");

        Dimension dimensionAppName = new Dimension()
                .withName("ApplicationName")
                .withValue(appNameWithDefault);

        Dimension dimensionInstanceId = new Dimension()
                .withName("InstanceId")
                .withValue(getInstanceIdWrapper());

        Set<Dimension> dimensionSet = new HashSet();
        dimensionSet.add(dimensionAppName);
        dimensionSet.add(dimensionInstanceId);

        MetricDatum datum = new MetricDatum()
                .withMetricName(metricName)
                .withUnit(unit)
                .withValue(value)
                .withDimensions()
                .withDimensions(dimensionSet);

        PutMetricDataRequest request = new PutMetricDataRequest()
                .withNamespace("ingester-monitoring")
                .withMetricData(datum);

        PutMetricDataResult response = cw.putMetricData(request);

        if (response.getSdkHttpMetadata().getHttpStatusCode() != 200) {
            logger.warn("Failed pushing CloudWatch Metric with RequestId: " + response.getSdkResponseMetadata().getRequestId());
            logger.debug("Response Status code: " + response.getSdkHttpMetadata().getHttpStatusCode());
        }

        return response;

    }
}
