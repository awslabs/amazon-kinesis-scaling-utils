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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.Statistic;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.scaling.AlreadyOneShardException;
import com.amazonaws.services.kinesis.scaling.ScalingOperationReport;
import com.amazonaws.services.kinesis.scaling.StreamScaler;
import com.amazonaws.services.kinesis.scaling.StreamScalingUtils;
import com.amazonaws.services.sns.AmazonSNSClient;

public class StreamMonitor implements Runnable {
	private final Log LOG = LogFactory.getLog(StreamMonitor.class);

	private AmazonKinesisClient kinesisClient;

	private AmazonCloudWatch cloudWatchClient;

	private AmazonSNSClient snsClient;

	public static final int TIMEOUT_SECONDS = 45;

	public static final int CLOUDWATCH_PERIOD = 60;

	private AutoscalingConfiguration config;

	@SuppressWarnings("unused")
	private volatile boolean keepRunning = true;

	private DateTime lastScaleDown = null;

	private StreamScaler scaler = null;

	private Exception exception;

	/* incomplete constructor only for testing */
	protected StreamMonitor(AutoscalingConfiguration config, StreamScaler scaler) throws Exception {
		this.config = config;
		this.scaler = scaler;
	}

	public StreamMonitor(AutoscalingConfiguration config, ExecutorService executor) throws Exception {
		this.config = config;
		Region setRegion = Region.getRegion(Regions.fromName(this.config.getRegion()));
		this.scaler = new StreamScaler(setRegion);
		this.cloudWatchClient = new AmazonCloudWatchClient(new DefaultAWSCredentialsProviderChain());
		this.cloudWatchClient.setRegion(setRegion);

		this.kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
		this.kinesisClient.setRegion(setRegion);

		this.snsClient = new AmazonSNSClient(new DefaultAWSCredentialsProviderChain());
		this.snsClient.setRegion(setRegion);
	}

	public void stop() {
		this.keepRunning = false;
		this.kinesisClient.shutdown();
		this.cloudWatchClient.shutdown();
		// the idle-connection-reaper is causing a thread leak without an
		// explicit shutdown
		IdleConnectionReaper.shutdown();
		LOG.info(String.format("Signalling Monitor for Stream %s to Stop", config.getStreamName()));
	}

	/**
	 * method which returns the current max capacity of a Stream based on
	 * configuration of alarms on PUTS or GETS
	 */
	protected StreamMetrics getStreamMaxCapacity() throws Exception {
		LOG.debug(String.format("Refreshing Stream %s Throughput Information", this.config.getStreamName()));
		Integer openShards = StreamScalingUtils.getOpenShardCount(this.kinesisClient, this.config.getStreamName());

		int maxBytes = openShards.intValue() * config.getScaleOnOperation().getMaxCapacity().get(StreamMetric.Bytes);
		int maxRecords = openShards.intValue()
				* config.getScaleOnOperation().getMaxCapacity().get(StreamMetric.Records);
		StreamMetrics maxCapacity = new StreamMetrics();
		maxCapacity.put(StreamMetric.Bytes, maxBytes);
		maxCapacity.put(StreamMetric.Records, maxRecords);

		LOG.debug(String.format("Stream Capacity %s Open Shards, %,d Bytes/Second, %d Records/Second", openShards,
				maxBytes, maxRecords));
		return maxCapacity;
	}

	/*
	 * generate a single cloudwatch request for GET operations, as this is
	 * instrumented as a single GetRecords metric, or create two for PUT as this
	 * is instrumented as both PutRecord and PutRecords
	 */
	private List<GetMetricStatisticsRequest> getCloudwatchRequests(KinesisOperationType operationType) {
		List<GetMetricStatisticsRequest> reqs = new ArrayList<>();
		// configure cloudwatch to determine the current stream metrics
		// add the stream name dimension
		// TODO: this should come from a configuration
		List<String> fetchMetrics = this.config.getScaleOnOperation().getMetricsToFetch();

		for (String s : fetchMetrics) {
			GetMetricStatisticsRequest cwRequest = new GetMetricStatisticsRequest();

			cwRequest.withNamespace("AWS/Kinesis")
					.withDimensions(new Dimension().withName("StreamName").withValue(this.config.getStreamName()))
					.withPeriod(CLOUDWATCH_PERIOD).withStatistics(Statistic.Sum);

			cwRequest.withMetricName(s);

			reqs.add(cwRequest);
		}

		return reqs;
	}

	/* method has bee lifted out of run() for unit testing purposes */
	protected ScalingOperationReport processCloudwatchMetrics(Map<StreamMetric, Map<Datapoint, Double>> metricsMap,
			StreamMetrics streamMaxCapacity, int cwSampleDuration, DateTime now) {

		ScalingOperationReport report = null;
		Map<StreamMetric, ScaleDirection> scaleDirectionPerMetric = new HashMap<StreamMetric, ScaleDirection>();

		for (StreamMetric metric : metricsMap.keySet()) {

			ScaleDirection scaleDirection = null;

			double currentMax = 0D;
			double currentPct = 0D;
			double latestPct = 0d;
			double latestMax = 0d;
			int lowSamples = 0;
			int highSamples = 0;
			DateTime lastTime = null;

			Map<Datapoint, Double> metrics = metricsMap.get(metric);

			// if we got nothing back, then there are no puts or gets
			// happening, so this is a full 'low sample'
			if (metrics.size() == 0) {
				lowSamples = this.config.getScaleDown().getScaleAfterMins();
			}

			// process the data point aggregates retrieved from CloudWatch
			// and log scale up/down votes by period
			for (Datapoint d : metrics.keySet()) {
				currentMax = metrics.get(d);
				currentPct = currentMax / streamMaxCapacity.get(metric);
				// keep track of the last measures
				if (lastTime == null || new DateTime(d.getTimestamp()).isAfter(lastTime)) {
					latestPct = currentPct;
					latestMax = currentMax;
				}
				lastTime = new DateTime(d.getTimestamp());

				// if the pct for the datapoint exceeds or is below the
				// thresholds, then add low/high samples
				if (currentPct > new Double(this.config.getScaleUp().getScaleThresholdPct()) / 100) {
					LOG.debug(String.format("%s: Cached High Alarm Condition for %.2f %s/Second (%.2f%%)", metric,
							currentMax, metric, currentPct * 100));
					highSamples++;
				} else if (currentPct < new Double(this.config.getScaleDown().getScaleThresholdPct()) / 100) {
					LOG.debug(String.format("%s: Cached Low Alarm Condition for %.2f %s/Second (%.2f%%)", metric,
							currentMax, metric, currentPct * 100));
					lowSamples++;
				}
			}

			// add low samples for the periods which we didn't get any
			// data points, if there are any
			if (metrics.size() < cwSampleDuration) {
				lowSamples += cwSampleDuration - metrics.size();
			}

			LOG.info(String.format(metric + ": Stream Used Capacity %.2f%% (%,.0f " + metric + " of %d)",
					latestPct * 100, latestMax, streamMaxCapacity.get(metric)));

			// check how many samples we have in the last period, and
			// flag the appropriate action
			if (highSamples >= config.getScaleUp().getScaleAfterMins()) {
				scaleDirection = ScaleDirection.UP;
			} else if (lowSamples >= config.getScaleDown().getScaleAfterMins()) {
				scaleDirection = ScaleDirection.DOWN;
			}

			LOG.debug(metric + ": Currently tracking " + highSamples + " Scale Up Alarms, and " + lowSamples
					+ " Scale Down Alarms. ScaleDirection: " + (scaleDirection != null ? scaleDirection : "None"));

			scaleDirectionPerMetric.put(metric, scaleDirection);
		}

		int scaleDownDirectionCount = 0;
		int scaleUpDirectionCount = 0;
		for (ScaleDirection scaleDirection : scaleDirectionPerMetric.values()) {
			if (scaleDirection == null) {
				continue;
			}
			if (scaleDirection.equals(ScaleDirection.UP)) {
				scaleUpDirectionCount++;
			} else {
				scaleDownDirectionCount++;
			}
		}

		ScaleDirection aggregatedScaleDirection = null;
		if (scaleUpDirectionCount > 0) {
			aggregatedScaleDirection = ScaleDirection.UP;
		} else if (scaleDownDirectionCount == scaleDirectionPerMetric.values().size()) {
			aggregatedScaleDirection = ScaleDirection.DOWN;
		}

		LOG.debug("Scale Directions: " + scaleDirectionPerMetric + ". Aggregated Scale Direction: "
				+ ((aggregatedScaleDirection != null) ? aggregatedScaleDirection : "None"));

		LOG.debug("Scale Directions: " + scaleDirectionPerMetric + ". Aggregated Scale Direction: "
				+ ((aggregatedScaleDirection != null) ? aggregatedScaleDirection : "None"));

		if (aggregatedScaleDirection == null) {
			// scale direction not set, so we're not going to scale
			// up or down - everything fine
			LOG.debug("No Scaling Directive received");
			return report;
		}

		try {
			// if the metric stats indicate a scale up or down, then do the
			// action
			if (aggregatedScaleDirection.equals(ScaleDirection.UP)) {
				// submit a scale up task
				Integer scaleUpCount = this.config.getScaleUp().getScaleCount();

				LOG.info(
						String.format("Requesting Scale Up of Stream %s by %s as %s has been above %s%% for %s Minutes",
								this.config.getStreamName(),
								(scaleUpCount != null) ? scaleUpCount : this.config.getScaleUp().getScalePct() + "%",
								this.config.getScaleOnOperation(), this.config.getScaleUp().getScaleThresholdPct(),
								this.config.getScaleUp().getScaleAfterMins()));

				if (scaleUpCount != null) {
					report = this.scaler.scaleUp(this.config.getStreamName(), scaleUpCount, this.config.getMinShards(),
							this.config.getMaxShards());
				} else {
					report = this.scaler.scaleUp(this.config.getStreamName(),
							new Double(this.config.getScaleUp().getScalePct()) / 100, this.config.getMinShards(),
							this.config.getMaxShards());

				}

				// send SNS notifications
				if (this.config.getScaleUp().getNotificationARN() != null) {
					StreamScalingUtils.sendNotification(this.snsClient, this.config.getScaleUp().getNotificationARN(),
							"Kinesis Autoscaling - Scale Up", report.toString());
				}
			} else if (aggregatedScaleDirection.equals(ScaleDirection.DOWN)) {
				// check the cool down interval
				if (lastScaleDown != null
						&& now.minusMinutes(this.config.getScaleDown().getCoolOffMins()).isBefore(lastScaleDown)) {
					LOG.info(String.format("Deferring Scale Down until Cool Off Period of %s Minutes has elapsed",
							this.config.getScaleDown().getCoolOffMins()));
				} else {
					// submit a scale down

					Integer scaleDownCount = this.config.getScaleDown().getScaleCount();
					LOG.info(String
							.format("Requesting Scale Down of Stream %s by %s as %s has been below %s%% for %s Minutes",
									this.config.getStreamName(),
									(scaleDownCount != null) ? scaleDownCount
											: this.config.getScaleUp().getScalePct() + "%",
									config.getScaleOnOperation(), this.config.getScaleDown().getScaleThresholdPct(),
									this.config.getScaleDown().getScaleAfterMins()));
					try {
						if (scaleDownCount != null) {
							report = this.scaler.scaleDown(this.config.getStreamName(), scaleDownCount,
									this.config.getMinShards(), this.config.getMaxShards());
						} else {
							report = this.scaler.scaleDown(this.config.getStreamName(),
									new Double(this.config.getScaleDown().getScalePct()) / 100,
									this.config.getMinShards(), this.config.getMaxShards());
						}

						lastScaleDown = new DateTime(System.currentTimeMillis());

						// send SNS notifications
						if (this.config.getScaleDown().getNotificationARN() != null) {
							StreamScalingUtils.sendNotification(this.snsClient,
									this.config.getScaleDown().getNotificationARN(), "Kinesis Autoscaling - Scale Down",
									report.toString());
						}
					} catch (AlreadyOneShardException aose) {
						// do nothing - we're already at 1 shard
						LOG.info("Not Scaling Down - Already at Minimum of 1 Shard");
					}
				}
			}
		} catch (Exception e) {
			LOG.error(e);
		}

		return report;
	}

	@Override
	public void run() {
		LOG.info(String.format("Started Stream Monitor for %s", config.getStreamName()));
		DateTime lastShardCapacityRefreshTime = new DateTime(System.currentTimeMillis());

		// determine shard capacity on the metric we will scale on
		StreamMetrics streamMaxCapacity;
		try {
			streamMaxCapacity = getStreamMaxCapacity();
		} catch (Exception e) {
			this.exception = e;
			return;
		}

		int cwSampleDuration = Math.max(config.getScaleUp().getScaleAfterMins(),
				config.getScaleDown().getScaleAfterMins());

		// add the metric name dimension
		List<GetMetricStatisticsRequest> cwRequests = getCloudwatchRequests(config.getScaleOnOperation());

		try {
			ScalingOperationReport report = null;

			do {
				DateTime now = new DateTime(System.currentTimeMillis());
				DateTime metricEndTime = new DateTime(System.currentTimeMillis());

				// fetch only the last N minutes metrics
				DateTime metricStartTime = metricEndTime.minusMinutes(cwSampleDuration);

				Map<StreamMetric, Map<Datapoint, Double>> metricsMap = new HashMap<StreamMetric, Map<Datapoint, Double>>();
				for (StreamMetric m : StreamMetric.values()) {
					metricsMap.put(m, new HashMap<Datapoint, Double>());
				}

				// iterate through all the requested CloudWatch metrics (either
				// a single GetRecords, or two: PutRecord and PutRecords and
				// collapse them down into a single map of sum metrics indexed
				// by
				// datapoint
				//
				// TODO Figure out how to mock this bit
				for (GetMetricStatisticsRequest req : cwRequests) {
					double sampleMetric = 0D;

					req.withStartTime(metricStartTime.toDate()).withEndTime(metricEndTime.toDate());

					// call cloudwatch to get the required metrics
					LOG.debug(String.format("Requesting %s minutes of CloudWatch Data for Stream Metric %s",
							cwSampleDuration, req.getMetricName()));
					GetMetricStatisticsResult cloudWatchMetrics = cloudWatchClient.getMetricStatistics(req);

					// aggregate the sample metrics by datapoint into a map, so
					// that PutRecords and PutRecord measures are added together
					for (Datapoint d : cloudWatchMetrics.getDatapoints()) {
						StreamMetric metric = StreamMetric.fromUnit(d.getUnit());

						Map<Datapoint, Double> metrics = metricsMap.get(metric);
						if (metrics.containsKey(d)) {
							sampleMetric = metrics.get(d);
						} else {
							sampleMetric = 0d;
						}
						sampleMetric += (d.getSum() / CLOUDWATCH_PERIOD);
						metrics.put(d, sampleMetric);
					}
				}

				// process the aggregated set of Cloudwatch Datapoints
				report = processCloudwatchMetrics(metricsMap, streamMaxCapacity, cwSampleDuration, now);

				if (report != null) {
					// refresh the current max capacity after the
					// modification
					streamMaxCapacity = getStreamMaxCapacity();
					lastShardCapacityRefreshTime = now;
				}

				if (report != null) {
					if (this.config.getScalingOperationReportListener() != null) {
						this.config.getScalingOperationReportListener().onReport(report);

					}
					LOG.info(report.toString());
					report = null;
				}

				// refresh shard stats every configured period, in case someone
				// has manually updated the number of shards
				if (now.minusMinutes(this.config.getRefreshShardsNumberAfterMin())
						.isAfter(lastShardCapacityRefreshTime)) {
					streamMaxCapacity = getStreamMaxCapacity();
					lastShardCapacityRefreshTime = now;
				}

				try {
					LOG.debug("Sleep");
					Thread.sleep(TIMEOUT_SECONDS * 1000);
				} catch (InterruptedException e) {
					LOG.error(e);
					break;
				}
			} while (keepRunning);

			LOG.info(String.format("Stream Monitor for %s in %s Completed. Exiting.", this.config.getStreamName(),
					this.config.getRegion()));
		} catch (Exception e) {
			this.exception = e;
		}
	}

	public void throwExceptions() throws Exception {
		if (this.exception != null)
			throw this.exception;
	}

	public Exception getException() {
		return this.exception;
	}

	protected void setLastScaleDown(DateTime setLastScaleDown) {
		this.lastScaleDown = setLastScaleDown;
	}

	AutoscalingConfiguration getConfig() {
		return this.config;
	}
}
