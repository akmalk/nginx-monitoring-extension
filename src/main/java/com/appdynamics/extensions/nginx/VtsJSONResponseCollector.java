/*
 * Copyright 2018. AppDynamics LLC and its affiliates.
 * All Rights Reserved.
 * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 * The copyright notice above does not evidence any actual or intended publication of such source code.
 *
 */
package com.appdynamics.extensions.nginx;

import com.appdynamics.extensions.MetricWriteHelper;
import com.appdynamics.extensions.conf.MonitorContextConfiguration;
import com.appdynamics.extensions.metrics.Metric;
import com.appdynamics.extensions.metrics.PerMinValueCalculator;
import com.appdynamics.extensions.nginx.Config.MetricConfig;
import com.appdynamics.extensions.nginx.Config.Stat;
import com.appdynamics.extensions.util.AssertUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static com.appdynamics.extensions.nginx.Constant.METRIC_SEPARATOR;

public class VtsJSONResponseCollector implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(VtsJSONResponseCollector.class);

    private Stat stat;

    private MonitorContextConfiguration configuration;

    private MetricWriteHelper metricWriteHelper;

    private String url;

    private static ObjectMapper objectMapper = new ObjectMapper();

    private String metricPrefix;

    private AtomicInteger heartBeat;

    private Phaser phaser;

    private static PerMinValueCalculator rpmCalculator = new PerMinValueCalculator();


    public VtsJSONResponseCollector(Stat stat, MonitorContextConfiguration configuration, MetricWriteHelper metricWriteHelper, String metricPrefix, String url, AtomicInteger heartBeat, Phaser phaser) {
        this.stat = stat;
        this.configuration = configuration;
        this.metricWriteHelper = metricWriteHelper;
        this.url = url;
        this.metricPrefix = metricPrefix + METRIC_SEPARATOR;
        this.heartBeat = heartBeat;
        this.phaser = phaser;
        this.phaser.register();
    }


    public void run() {
        CloseableHttpClient httpClient = configuration.getContext().getHttpClient();
        CloseableHttpResponse response = null;
        List<Metric> metricList = Lists.newArrayList();
        try {
            HttpGet get = new HttpGet(url);
            response = httpClient.execute(get);
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity, "UTF-8");
            if (heartBeat.get() == 0)
                heartBeat.incrementAndGet();
            AssertUtils.assertNotNull(responseBody, "response of the request is empty");
            String header = response.getFirstHeader("Content-Type").getValue();
            if (header != null && header.contains("application/json")) {
                JSONObject jsonObject = new JSONObject(responseBody);
                if (stat.getMetricConfig() != null) {
                    metricList.addAll(collectMetrics(jsonObject, stat.getMetricConfig()));
                }
            }
            logger.debug("Successfully collected metrics for Stat {} {}", url,stat.getName());
        } catch (Exception e) {
            logger.error("Unexpected error while collecting metrics for stat", e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            metricWriteHelper.transformAndPrintMetrics(metricList);
            phaser.arriveAndDeregister();
        }
    }

    private List<Metric> collectMetrics(JSONObject jsonObject, MetricConfig[] metricConfig) {
        List<Metric> metrics = new ArrayList<>();

        // collect main server connections metrics
        JSONObject json = jsonObject.getJSONObject("connections");
        metrics.addAll(collectObjectMetrics(json, "Connections", metricPrefix, metricConfig));

        // collect serverZones metrics
        json = jsonObject.getJSONObject("serverZones");
        Set<String> serverZoneNames = json.keySet();
        for (String serverZoneName : serverZoneNames) {
            if(serverZoneName.equals("*")) continue;
            JSONObject zoneObject = json.getJSONObject(serverZoneName);
            metrics.addAll(collectObjectMetrics(zoneObject, "Server Zones" + METRIC_SEPARATOR + serverZoneName,
                    metricPrefix, metricConfig));
            JSONObject responses = zoneObject.getJSONObject("responses");
            metrics.addAll(collectObjectMetrics(responses, "Server Zones" + METRIC_SEPARATOR + serverZoneName +
                            METRIC_SEPARATOR + "Responses",
                    metricPrefix, metricConfig));
        }

        // collect upstreamZones metrics
        json = jsonObject.getJSONObject("upstreamZones");
        Set<String> upstreamZones = json.keySet();
        for (String upstreamZoneName : upstreamZones) {
            JSONArray serverGroups = json.getJSONArray(upstreamZoneName);
            for (int i = 0; i < serverGroups.length(); i++) {
                JSONObject server = serverGroups.getJSONObject(i);
                metrics.addAll(collectObjectMetrics(server,
                        "Upstreams|" + upstreamZoneName.replace(":", "") + METRIC_SEPARATOR +
                                server.getString("server").replace(":", "-"),
                        metricPrefix, metricConfig));
                JSONObject responses = server.getJSONObject("responses");
                metrics.addAll(collectObjectMetrics(responses,
                        "Upstreams|" + upstreamZoneName.replace(":", "") + METRIC_SEPARATOR +
                                server.getString("server").replace(":", "-") +
                                METRIC_SEPARATOR + "Responses",
                        metricPrefix, metricConfig));
            }
        }
        return metrics;
    }

    private List<Metric> collectObjectMetrics(JSONObject jsonObject, String configStr, String metricPrefix, MetricConfig[] metricConfig) {
        List<Metric> metrics = new ArrayList<>();
        for (MetricConfig config : metricConfig) {
            if(!jsonObject.has(config.getAttr())) continue;
            Map<String, String> propertiesMap = objectMapper.convertValue(config, Map.class);
            Metric metric = new Metric(config.getAlias(), String.valueOf(jsonObject.get(config.getAttr())),
                    metricPrefix + configStr + METRIC_SEPARATOR + config.getAlias(),
                    propertiesMap);
            metrics.add(metric);
        }
        return metrics;
    }
}

