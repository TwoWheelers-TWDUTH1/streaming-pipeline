package com.tw.utils;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.http.HttpResponse;

import com.tw.services.ApiProducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(locations = "classpath:test.properties")
public class CloudWatchUtilTest {

    @InjectMocks
    private CloudWatchUtil cloudWatchUtil;

    @Mock
    private Logger logger;

    @Mock
    private AmazonCloudWatch cw;

    @Value("${spring.profiles.active}")
    private String testAppName;

    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        Field appNameField = CloudWatchUtil.class.getDeclaredField("appName");
        appNameField.setAccessible(true);
        appNameField.set(cloudWatchUtil, testAppName);

        PutMetricDataResult expectedResponse = Mockito.mock(PutMetricDataResult.class);

        SdkHttpMetadata sdkHttpMetadataMock = SdkHttpMetadata.from( new HttpResponse(null, null) {
            @Override
            public int getStatusCode() {
                return 200;
            }
        });
        when(expectedResponse.getSdkHttpMetadata()).thenReturn(sdkHttpMetadataMock);

        when(cw.putMetricData(any())).thenReturn(expectedResponse);
    }

    @Test
    public void shouldPutMetricSendCorrectDataToCloudWatch() {

        String testMetricName = "testMetric";
        Double testMetricValue = 1.0;
        StandardUnit testMetricUnit = StandardUnit.None;

        cloudWatchUtil.putMetric(testMetricName, testMetricValue, testMetricUnit);

        Dimension expectedDimensionAppName = new Dimension()
                .withName("ApplicationName")
                .withValue("test_app_name");

        Dimension expectedDimensionInstanceId = new Dimension()
                .withName("InstanceId")
                .withValue("InstanceId");

        Set<Dimension> expectedDimensionSet = new HashSet<>();
        expectedDimensionSet.add(expectedDimensionAppName);
        expectedDimensionSet.add(expectedDimensionInstanceId);

        MetricDatum datum = new MetricDatum()
                .withMetricName(testMetricName)
                .withUnit(testMetricUnit)
                .withValue(testMetricValue)
                .withDimensions(expectedDimensionSet);

        PutMetricDataRequest expectedRequest = new PutMetricDataRequest()
                .withNamespace("APP/Monitoring")
                .withMetricData(datum);

        verify(cw).putMetricData(expectedRequest);
    }

    @Test
    public void shouldSendHeartBeatSendCorrectDataToCloudWatch() {

        cloudWatchUtil.sendHeartBeat();

        Dimension expectedDimensionAppName = new Dimension()
                .withName("ApplicationName")
                .withValue("test_app_name");

        Dimension expectedDimensionInstanceId = new Dimension()
                .withName("InstanceId")
                .withValue("InstanceId");

        Set<Dimension> expectedDimensionSet = new HashSet<>();
        expectedDimensionSet.add(expectedDimensionAppName);
        expectedDimensionSet.add(expectedDimensionInstanceId);

        MetricDatum datum = new MetricDatum()
                .withMetricName("is_app_running")
                .withUnit(StandardUnit.None)
                .withValue(1.0)
                .withDimensions(expectedDimensionSet);

        PutMetricDataRequest expectedRequest = new PutMetricDataRequest()
                .withNamespace("APP/Monitoring")
                .withMetricData(datum);

        verify(cw).putMetricData(expectedRequest);
    }

}
