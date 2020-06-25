/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.scaling.StreamScalingUtils;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Datapoint;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsResponse;
import software.amazon.awssdk.services.cloudwatch.model.InvalidParameterCombinationException;
import software.amazon.awssdk.services.cloudwatch.model.InvalidParameterValueException;
import software.amazon.awssdk.services.cloudwatch.model.MissingRequiredParameterException;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/**
 * The StreamMetricsManager class is responsible for extracting current Stream
 * utilisation metrics using a CloudWatch client, as well as for being able to
 * extract the maximum capacity metrics for a Stream for the Kinesis Operation
 * Types that are to be tracked for automatic scaling purposes
 * 
 * @author meyersi
 *
 */
public class StreamMetricManager {
	private final Logger LOG = LoggerFactory.getLogger(StreamMetricManager.class);
	public final String CW_NAMESPACE = "AWS/Kinesis";
	private String streamName;
	private CloudWatchClient cloudWatchClient;
	private KinesisClient kinesisClient;
	private int cloudWatchPeriod;

	// the set of all Operations that will be tracked in cloudwatch
	private Set<KinesisOperationType> trackedOperations = new HashSet<>();

	// the current maximum capacity of the stream
	private Map<KinesisOperationType, StreamMetrics> streamMaxCapacity = new HashMap<>();

	// set of CloudWatch request template objects, which simplify extracting
	// metrics in future
	private Map<KinesisOperationType, List<GetMetricStatisticsRequest.Builder>> cloudwatchRequestTemplates = new HashMap<>();

	public StreamMetricManager(String streamName, List<KinesisOperationType> types, CloudWatchClient cloudWatchClient,
			KinesisClient kinesisClient) {
		this(streamName, StreamMonitor.CLOUDWATCH_PERIOD, types, cloudWatchClient, kinesisClient);
	}

	public StreamMetricManager(String streamName, int cloudWatchPeriod, List<KinesisOperationType> types,
			CloudWatchClient cloudWatchClient, KinesisClient kinesisClient) {
		this.streamName = streamName;
		this.trackedOperations.addAll(types);
		this.cloudWatchClient = cloudWatchClient;
		this.kinesisClient = kinesisClient;
		this.cloudWatchPeriod = cloudWatchPeriod;

		for (KinesisOperationType op : this.trackedOperations) {
			// create CloudWatch request templates for the information we have
			// at this point
			for (String metricName : op.getMetricsToFetch()) {
				GetMetricStatisticsRequest.Builder cwRequestBuilder = GetMetricStatisticsRequest.builder();

				cwRequestBuilder.namespace(CW_NAMESPACE)
						.dimensions(Dimension.builder().name("StreamName").value(this.streamName).build())
						.period(cloudWatchPeriod).statistics(Statistic.SUM).metricName(metricName);

				if (!this.cloudwatchRequestTemplates.containsKey(op)) {
					this.cloudwatchRequestTemplates.put(op, new ArrayList<GetMetricStatisticsRequest.Builder>() {
						{
							add(cwRequestBuilder);
						}
					});
				} else {
					this.cloudwatchRequestTemplates.get(op).add(cwRequestBuilder);
				}
			}
		}
	}

	public Map<KinesisOperationType, StreamMetrics> getStreamMaxCapacity() {
		return this.streamMaxCapacity;
	}

	/**
	 * Method which extracts and then caches the current max capacity of the stream.
	 * This is periodically refreshed by the client when needed
	 * 
	 * @throws Exception
	 */
	public void loadMaxCapacity() throws Exception {
		LOG.debug(String.format("Refreshing Stream %s Throughput Information", this.streamName));
		Integer openShards = StreamScalingUtils.getOpenShardCount(this.kinesisClient, this.streamName);

		for (KinesisOperationType op : this.trackedOperations) {
			int maxBytes = openShards.intValue() * op.getMaxCapacity().get(StreamMetric.Bytes);
			int maxRecords = openShards.intValue() * op.getMaxCapacity().get(StreamMetric.Records);
			StreamMetrics maxCapacity = new StreamMetrics(op);
			maxCapacity.put(StreamMetric.Bytes, maxBytes);
			maxCapacity.put(StreamMetric.Records, maxRecords);

			streamMaxCapacity.put(op, maxCapacity);

			LOG.debug(String.format("Stream Capacity %s Open Shards, %,d Bytes/Second, %d Records/Second", openShards,
					maxBytes, maxRecords));
		}
	}

	/**
	 * Method which extracts the current utilisation metrics for the operation types
	 * registered in the metrics manager
	 * 
	 * @param cwSampleDuration
	 * @param metricStartTime
	 * @param metricEndTime
	 * @return
	 * @throws Exception
	 */
	public Map<KinesisOperationType, Map<StreamMetric, Map<Datapoint, Double>>> queryCurrentUtilisationMetrics(
			int cwSampleDuration, DateTime metricStartTime, DateTime metricEndTime) throws Exception {
		Map<KinesisOperationType, Map<StreamMetric, Map<Datapoint, Double>>> currentUtilisationMetrics = new HashMap<>();

		// seed the current utilisation objects with default Maps to simplify
		// object creation later
		for (KinesisOperationType op : this.trackedOperations) {
			for (StreamMetric m : StreamMetric.values()) {
				currentUtilisationMetrics.put(op, new HashMap<StreamMetric, Map<Datapoint, Double>>() {
					{
						put(m, new HashMap<Datapoint, Double>());
					}
				});
			}
		}

		for (Map.Entry<KinesisOperationType, List<GetMetricStatisticsRequest.Builder>> entry : this.cloudwatchRequestTemplates
				.entrySet()) {
			for (GetMetricStatisticsRequest.Builder reqBuilder : this.cloudwatchRequestTemplates.get(entry.getKey())) {
				double sampleMetric = 0D;

				reqBuilder.startTime(metricStartTime.toDate().toInstant()).endTime(metricEndTime.toDate().toInstant());

				GetMetricStatisticsRequest req = reqBuilder.build();

				// call cloudwatch to get the required metrics
				LOG.debug(String.format("Requesting %s minutes of CloudWatch Data for Stream Metric %s",
						cwSampleDuration, req.metricName()));

				GetMetricStatisticsResponse cloudWatchMetrics = null;
				boolean ok = false;
				int tryCount = 1;
				long sleepCap = 2000;
				int tryCap = 20;
				while (!ok) {
					try {
						cloudWatchMetrics = this.cloudWatchClient.getMetricStatistics(req);
						ok = true;
					} catch (InvalidParameterValueException e) {
						throw e;
					} catch (MissingRequiredParameterException e) {
						throw e;
					} catch (InvalidParameterCombinationException e) {
						throw e;
					} catch (Exception e) {
						// this is probably just a transient error, so retry
						// after backoff
						tryCount++;
						if (tryCount >= tryCap) {
							throw e;
						}
						long sleepFor = new Double(Math.pow(2, tryCount) * 100).longValue();
						Thread.sleep(sleepFor > sleepCap ? sleepCap : sleepFor);
					}
				}

				// aggregate the sample metrics by datapoint into a map,
				// so that PutRecords and PutRecord measures are added
				// together
				for (Datapoint d : cloudWatchMetrics.datapoints()) {
					StreamMetric metric = StreamMetric.fromUnit(d.unit().name());

					Map<Datapoint, Double> metrics = currentUtilisationMetrics.get(entry.getKey()).get(metric);
					if (metrics == null) {
						metrics = new HashMap<>();
					}
					if (metrics.containsKey(d)) {
						sampleMetric = metrics.get(d);
					} else {
						sampleMetric = 0d;
					}
					sampleMetric += (d.sum() / cloudWatchPeriod);
					metrics.put(d, sampleMetric);
					currentUtilisationMetrics.get(entry.getKey()).put(metric, metrics);
				}

			}
		}

		return currentUtilisationMetrics;
	}
}
