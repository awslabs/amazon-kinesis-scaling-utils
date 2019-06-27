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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.InvalidParameterCombinationException;
import com.amazonaws.services.cloudwatch.model.InvalidParameterValueException;
import com.amazonaws.services.cloudwatch.model.MissingRequiredParameterException;
import com.amazonaws.services.cloudwatch.model.Statistic;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.scaling.StreamScalingUtils;

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
	private final Log LOG = LogFactory.getLog(StreamMetricManager.class);
	public final String CW_NAMESPACE = "AWS/Kinesis";
	private String streamName;
	private AmazonCloudWatch cloudWatchClient;
	private AmazonKinesisClient kinesisClient;

	// the set of all Operations that will be tracked in cloudwatch
	private Set<KinesisOperationType> trackedOperations = new HashSet<>();

	// the current maximum capacity of the stream
	private Map<KinesisOperationType, StreamMetrics> streamMaxCapacity = new HashMap<>();

	// set of CloudWatch request template objects, which simplify extracting
	// metrics in future
	private Map<KinesisOperationType, List<GetMetricStatisticsRequest>> cloudwatchRequestTemplates = new HashMap<>();

	public StreamMetricManager(String streamName, List<KinesisOperationType> types, AmazonCloudWatch cloudWatchClient,
			AmazonKinesisClient kinesisClient) {
		this.streamName = streamName;
		this.trackedOperations.addAll(types);
		this.cloudWatchClient = cloudWatchClient;
		this.kinesisClient = kinesisClient;

		for (KinesisOperationType op : this.trackedOperations) {
			// create CloudWatch request templates for the information we have
			// at this point
			for (String metricName : op.getMetricsToFetch()) {
				GetMetricStatisticsRequest cwRequest = new GetMetricStatisticsRequest();

				cwRequest.withNamespace(CW_NAMESPACE)
						.withDimensions(new Dimension().withName("StreamName").withValue(this.streamName))
						.withPeriod(StreamMonitor.CLOUDWATCH_PERIOD).withStatistics(Statistic.Sum)
						.withMetricName(metricName);

				if (!this.cloudwatchRequestTemplates.containsKey(op)) {
					this.cloudwatchRequestTemplates.put(op, new ArrayList<GetMetricStatisticsRequest>() {
						{
							add(cwRequest);
						}
					});
				} else {
					this.cloudwatchRequestTemplates.get(op).add(cwRequest);
				}
			}
		}
	}

	public Map<KinesisOperationType, StreamMetrics> getStreamMaxCapacity() {
		return this.streamMaxCapacity;
	}

	/**
	 * Method which extracts and then caches the current max capacity of the
	 * stream. This is periodically refreshed by the client when needed
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
	 * Method which extracts the current utilisation metrics for the operation
	 * types registered in the metrics manager
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

		for (Map.Entry<KinesisOperationType, List<GetMetricStatisticsRequest>> entry : this.cloudwatchRequestTemplates
				.entrySet()) {
			for (GetMetricStatisticsRequest req : this.cloudwatchRequestTemplates.get(entry.getKey())) {
				double sampleMetric = 0D;

				req.withStartTime(metricStartTime.toDate()).withEndTime(metricEndTime.toDate());

				// call cloudwatch to get the required metrics
				LOG.debug(String.format("Requesting %s minutes of CloudWatch Data for Stream Metric %s",
						cwSampleDuration, req.getMetricName()));

				GetMetricStatisticsResult cloudWatchMetrics = null;
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
				for (Datapoint d : cloudWatchMetrics.getDatapoints()) {
					StreamMetric metric = StreamMetric.fromUnit(d.getUnit());

					Map<Datapoint, Double> metrics = currentUtilisationMetrics.get(entry.getKey()).get(metric);
					if (metrics == null) {
						metrics = new HashMap<>();
					}
					if (metrics.containsKey(d)) {
						sampleMetric = metrics.get(d);
					} else {
						sampleMetric = 0d;
					}
					sampleMetric += (d.getSum() / StreamMonitor.CLOUDWATCH_PERIOD);
					metrics.put(d, sampleMetric);
					currentUtilisationMetrics.get(entry.getKey()).put(metric, metrics);
				}

			}
		}

		return currentUtilisationMetrics;
	}
}
