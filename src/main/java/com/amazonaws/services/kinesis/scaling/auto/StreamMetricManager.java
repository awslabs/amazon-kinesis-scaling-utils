/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.scaling.auto;

import java.util.ArrayList;
import java.util.EnumMap;
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
	private Map<KinesisOperationType, StreamMetrics> streamMaxCapacity = new EnumMap<>(KinesisOperationType.class);

	// set of CloudWatch request template objects, which simplify extracting
	// metrics in future
	private Map<KinesisOperationType, List<GetMetricStatisticsRequest>> cloudwatchRequestTemplates = new EnumMap<>(KinesisOperationType.class);

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
		Map<KinesisOperationType, Map<StreamMetric, Map<Datapoint, Double>>> currentUtilisationMetrics = new EnumMap<>(KinesisOperationType.class);

		// seed the current utilisation objects with default Maps to simplify
		// object creation later
		for (KinesisOperationType op : this.trackedOperations) {
			for (StreamMetric m : StreamMetric.values()) {
				currentUtilisationMetrics.put(op, new EnumMap<StreamMetric, Map<Datapoint, Double>>(StreamMetric.class) {
					{
						put(m, new HashMap<Datapoint, Double>());
					}
				});
			}
		}

		for (Map.Entry<KinesisOperationType, List<GetMetricStatisticsRequest>> entry : this.cloudwatchRequestTemplates.entrySet()) {
			for (GetMetricStatisticsRequest req : this.cloudwatchRequestTemplates.get(entry.getKey())) {
				double sampleMetric = 0D;

				req.withStartTime(metricStartTime.toDate()).withEndTime(metricEndTime.toDate());

				// call cloudwatch to get the required metrics
				LOG.debug(String.format("Requesting %s minutes of CloudWatch Data for Stream Metric %s",
						cwSampleDuration, req.getMetricName()));
				GetMetricStatisticsResult cloudWatchMetrics = this.cloudWatchClient.getMetricStatistics(req);

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