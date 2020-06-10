/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.javatuples.Triplet;
import org.joda.time.DateTime;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.scaling.AlreadyOneShardException;
import com.amazonaws.services.kinesis.scaling.ScaleDirection;
import com.amazonaws.services.kinesis.scaling.ScalingCompletionStatus;
import com.amazonaws.services.kinesis.scaling.ScalingOperationReport;
import com.amazonaws.services.kinesis.scaling.StreamScaler;
import com.amazonaws.services.kinesis.scaling.StreamScalingUtils;
import com.amazonaws.services.sns.AmazonSNSClient;

public class StreamMonitor implements Runnable {
	private final Log LOG = LogFactory.getLog(StreamMonitor.class);

	private AmazonKinesisClient kinesisClient;

	private AmazonCloudWatch cloudWatchClient;

	private AmazonSNSClient snsClient;

	public static final int CLOUDWATCH_PERIOD = 60;

	private AutoscalingConfiguration config;

	private volatile boolean keepRunning = true;

	private DateTime lastScaleDown = null;
	private DateTime lastScaleUp = null;

	private StreamScaler scaler = null;

	private Exception exception;

	/* incomplete constructor only for testing */
	protected StreamMonitor(AutoscalingConfiguration config, StreamScaler scaler) throws Exception {
		this.config = config;
		this.scaler = scaler;
	}

	public StreamMonitor(AutoscalingConfiguration config) throws Exception {
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

	/* method has been lifted out of run() for unit testing purposes */
	protected ScalingOperationReport processCloudwatchMetrics(
			Map<KinesisOperationType, Map<StreamMetric, Map<Datapoint, Double>>> currentUtilisationMetrics,
			Map<KinesisOperationType, StreamMetrics> streamMaxCapacity, int cwSampleDuration, DateTime now) {
		ScalingOperationReport report = null;
		ScaleDirection finalScaleDirection = null;

		// for each type of operation that the customer has requested profiling
		// (PUT, GET)
		Map<KinesisOperationType, ScaleDirection> scaleVotes = new HashMap<>();

		for (Map.Entry<KinesisOperationType, Map<StreamMetric, Map<Datapoint, Double>>> entry : currentUtilisationMetrics
				.entrySet()) {
			// set the default scaling vote to 'do nothing'
			scaleVotes.put(entry.getKey(), ScaleDirection.NONE);

			Map<StreamMetric, Triplet<Integer, Integer, Double>> perMetricSamples = new HashMap<>();
			StreamMetric higherUtilisationMetric;
			Double higherUtilisationPct;

			// process each metric type, including Records and Bytes
			for (StreamMetric metric : StreamMetric.values()) {
				double currentMax = 0D;
				double currentPct = 0D;
				double latestPct = 0d;
				double latestMax = 0d;
				double latestAvg = 0d;
				DateTime lastTime = null;
				int lowSamples = 0;
				int highSamples = 0;

				Map<Datapoint, Double> metrics = new HashMap<>();

				if (!currentUtilisationMetrics.containsKey(entry.getKey()) || !entry.getValue().containsKey(metric)) {
					// we have no samples for this type of metric which is ok -
					// they'll later be counted as low metrics
				} else {
					metrics = entry.getValue().get(metric);
				}

				// if we got nothing back, then there are no operations of the
				// given type happening, so this is a full 'low sample'
				if (metrics.size() == 0) {
					lowSamples = this.config.getScaleDown().getScaleAfterMins();
				}

				// process the data point aggregates retrieved from CloudWatch
				// and log scale up/down votes by period
				for (Map.Entry<Datapoint, Double> datapointEntry : metrics.entrySet()) {
					currentMax = datapointEntry.getValue();
					currentPct = currentMax / streamMaxCapacity.get(entry.getKey()).get(metric);
					// keep track of the last measures
					if (lastTime == null || new DateTime(datapointEntry.getKey().getTimestamp()).isAfter(lastTime)) {
						latestPct = currentPct;
						latestMax = currentMax;

						// latest average is a simple moving average
						latestAvg = latestAvg == 0d ? currentPct : (latestAvg + currentPct) / 2;
					}
					lastTime = new DateTime(datapointEntry.getKey().getTimestamp());

					// if the pct for the datapoint exceeds or is below the
					// thresholds, then add low/high samples
					if (currentPct > new Double(this.config.getScaleUp().getScaleThresholdPct()) / 100) {
						LOG.debug(String.format("%s %s: Cached High Alarm Condition for %.2f %s/Second (%.2f%%)",
								entry.getKey(), metric, currentMax, metric, currentPct * 100));
						highSamples++;
					} else if (currentPct < new Double(this.config.getScaleDown().getScaleThresholdPct()) / 100) {
						LOG.debug(String.format("%s %s: Cached Low Alarm Condition for %.2f %s/Second (%.2f%%)",
								entry.getKey(), metric, currentMax, metric, currentPct * 100));
						lowSamples++;
					}
				}

				// add low samples for the periods which we didn't get any
				// data points, if there are any
				if (metrics.size() < cwSampleDuration) {
					lowSamples += cwSampleDuration - metrics.size();
				}

				LOG.info(
						String.format(metric + ": Stream %s Used %s[%s] Capacity ~ %.2f%% (%,.0f " + metric + " of %d)",
								config.getStreamName(), entry.getKey(), metric, latestAvg * 100, latestMax,
								streamMaxCapacity.get(entry.getKey()).get(metric)));

				// merge the per-stream metric samples together for the
				// operation
				if (!perMetricSamples.containsKey(metric)) {
					// create a new sample entry
					perMetricSamples.put(metric, new Triplet<>(highSamples, lowSamples, latestAvg));
				} else {
					// merge the samples
					Triplet<Integer, Integer, Double> previousHighLow = perMetricSamples.get(metric);
					Triplet<Integer, Integer, Double> newHighLow = new Triplet<>(
							previousHighLow.getValue0() + highSamples, previousHighLow.getValue1() + lowSamples,
							(previousHighLow.getValue2() + latestAvg) / 2);
					perMetricSamples.put(metric, newHighLow);
				}
			}

			/*-
			 * we now have per metric samples for this operation type
			 * 
			 * For Example: 
			 * 
			 * Metric  | High Samples | Low Samples | Pct Used
			 * Bytes   | 3            | 0           | .98
			 * Records | 0            | 10          | .2
			 * 
			 * Check these values against the provided configuration. If we have
			 * been above the 'scaleAfterMins' with high samples for either
			 * metric, then we scale up. If not, then if we've been below the
			 * scaleAfterMins with low samples, then we scale down. Otherwise
			 * the vote stays as NONE
			 */

			// first find out which of the dimensions of stream utilisation are
			// higher - we'll use the higher of the two for time checks
			if (perMetricSamples.get(StreamMetric.Bytes).getValue2() >= perMetricSamples.get(StreamMetric.Records)
					.getValue2()) {
				higherUtilisationMetric = StreamMetric.Bytes;
				higherUtilisationPct = perMetricSamples.get(StreamMetric.Bytes).getValue2();
			} else {
				higherUtilisationMetric = StreamMetric.Records;
				higherUtilisationPct = perMetricSamples.get(StreamMetric.Records).getValue2();
			}

			LOG.info(String.format(
					"Will decide scaling action based on metric %s[%s] due to higher utilisation metric %.2f%%",
					entry.getKey(), higherUtilisationMetric, higherUtilisationPct * 100));

			if (perMetricSamples.get(higherUtilisationMetric).getValue0() >= config.getScaleUp().getScaleAfterMins()) {
				scaleVotes.put(entry.getKey(), ScaleDirection.UP);
			} else if (perMetricSamples.get(higherUtilisationMetric).getValue1() >= config.getScaleDown()
					.getScaleAfterMins()) {
				scaleVotes.put(entry.getKey(), ScaleDirection.DOWN);
			}
		}

		// process the scaling votes
		ScaleDirection getVote = scaleVotes.get(KinesisOperationType.GET);
		ScaleDirection putVote = scaleVotes.get(KinesisOperationType.PUT);

		// check if we have both get and put votes - if we have both then
		// implement the decision matrix
		if (getVote != null && putVote != null) {
			// if either of the votes are to scale up, then do so. If both are
			// None,
			// then do nothing. Otherwise scale down
			if (getVote == ScaleDirection.UP || putVote == ScaleDirection.UP) {
				finalScaleDirection = ScaleDirection.UP;
			} else if (getVote == ScaleDirection.NONE && putVote == ScaleDirection.NONE) {
				finalScaleDirection = ScaleDirection.NONE;
			} else {
				finalScaleDirection = ScaleDirection.DOWN;
			}
		} else {
			// we only have get or put votes, so use the non-null one
			finalScaleDirection = (getVote == null ? putVote : getVote);
		}

		try {
			int currentShardCount = this.scaler.getOpenShardCount(this.config.getStreamName());

			// if the metric stats indicate a scale up or down, then do the
			// action
			if (finalScaleDirection.equals(ScaleDirection.UP)) {
				// check the cool down interval
				if (lastScaleUp != null
						&& now.minusMinutes(this.config.getScaleUp().getCoolOffMins()).isBefore(lastScaleUp)) {
					LOG.info(String.format(
							"Stream %s: Deferring Scale Up until Cool Off Period of %s Minutes has elapsed",
							this.config.getStreamName(), this.config.getScaleUp().getCoolOffMins()));
				} else {
					// submit a scale up task
					Integer scaleUpCount = this.config.getScaleUp().getScaleCount();

					LOG.info(String.format(
							"Requesting Scale Up of Stream %s by %s as %s has been above %s%% for %s Minutes",
							this.config.getStreamName(),
							(scaleUpCount != null) ? scaleUpCount : this.config.getScaleUp().getScalePct() + "%",
							this.config.getScaleOnOperations().toString(), this.config.getScaleUp().getScaleThresholdPct(),
							this.config.getScaleUp().getScaleAfterMins()));

					if (scaleUpCount != null) {
						report = this.scaler.updateShardCount(this.config.getStreamName(), currentShardCount,
								currentShardCount + scaleUpCount, this.config.getMinShards(), this.config.getMaxShards(),
								false);
					} else {
						report = this.scaler.updateShardCount(this.config.getStreamName(), currentShardCount,
								new Double(currentShardCount * (new Double(this.config.getScaleUp().getScalePct()) / 100))
										.intValue(),
								this.config.getMinShards(), this.config.getMaxShards(), false);

					}

					lastScaleUp = new DateTime(System.currentTimeMillis());

					// send SNS notifications
					if (report != null && this.config.getScaleUp().getNotificationARN() != null && this.snsClient != null) {
						StreamScalingUtils.sendNotification(this.snsClient, this.config.getScaleUp().getNotificationARN(),
								"Kinesis Autoscaling - Scale Up", (report == null ? "No Changes Made" : report.asJson()));
					}
				}
			} else if (finalScaleDirection.equals(ScaleDirection.DOWN)) {
				// check the cool down interval
				if (lastScaleDown != null
						&& now.minusMinutes(this.config.getScaleDown().getCoolOffMins()).isBefore(lastScaleDown)) {
					LOG.info(String.format(
							"Stream %s: Deferring Scale Down until Cool Off Period of %s Minutes has elapsed",
							this.config.getStreamName(), this.config.getScaleDown().getCoolOffMins()));
				} else {
					// submit a scale down
					Integer scaleDownCount = this.config.getScaleDown().getScaleCount();
					LOG.info(String.format(
							"Requesting Scale Down of Stream %s by %s as %s has been below %s%% for %s Minutes",
							this.config.getStreamName(),
							(scaleDownCount != null) ? scaleDownCount : this.config.getScaleDown().getScalePct() + "%",
							config.getScaleOnOperations().toString(), this.config.getScaleDown().getScaleThresholdPct(),
							this.config.getScaleDown().getScaleAfterMins()));
					try {
						if (scaleDownCount != null) {
							report = this.scaler.updateShardCount(this.config.getStreamName(), currentShardCount,
									currentShardCount - scaleDownCount, this.config.getMinShards(),
									this.config.getMaxShards(), false);
						} else {
							report = this.scaler.updateShardCount(this.config.getStreamName(), currentShardCount,
									new Double(currentShardCount
											* (new Double(this.config.getScaleDown().getScalePct()) / 100)).intValue(),
									this.config.getMinShards(), this.config.getMaxShards(), false);
						}

						lastScaleDown = new DateTime(System.currentTimeMillis());

						// send SNS notifications
						if (report != null && this.config.getScaleDown().getNotificationARN() != null
								&& this.snsClient != null) {
							StreamScalingUtils.sendNotification(this.snsClient,
									this.config.getScaleDown().getNotificationARN(), "Kinesis Autoscaling - Scale Down",
									(report == null ? "No Changes Made" : report.asJson()));
						}
					} catch (AlreadyOneShardException aose) {
						// do nothing - we're already at 1 shard
						LOG.info(String.format("Stream %s: Not Scaling Down - Already at Minimum of 1 Shard",
								this.config.getStreamName()));
					}
				}
			} else {
				// scale direction not set, so we're not going to scale
				// up or down - everything fine
				LOG.info("No Scaling required - Stream capacity within specified tolerances");
				return this.scaler.reportFor(ScalingCompletionStatus.NoActionRequired, this.config.getStreamName(), 0,
						finalScaleDirection);
			}
		} catch (Exception e) {
			LOG.error("Failed to process stream " + this.config.getStreamName(), e);
		}

		return report;
	}

	@Override
	public void run() {
		LOG.info(String.format("Started Stream Monitor for %s", config.getStreamName()));
		DateTime lastShardCapacityRefreshTime = new DateTime(System.currentTimeMillis());

		// create a StreamMetricManager object
		StreamMetricManager metricManager = new StreamMetricManager(this.config.getStreamName(),
				this.config.getScaleOnOperations(), this.cloudWatchClient, this.kinesisClient);

		LOG.info(String.format("Using Stream Scaler Version %s", StreamScaler.version));

		try {
			// load the current configured max capacity
			metricManager.loadMaxCapacity();

			// configure the duration to request from cloudwatch
			int cwSampleDuration = Math.max(config.getScaleUp().getScaleAfterMins(),
					config.getScaleDown().getScaleAfterMins());

			ScalingOperationReport report = null;

			do {
				DateTime now = new DateTime(System.currentTimeMillis());
				DateTime metricEndTime = now;

				// fetch only the last N minutes metrics
				DateTime metricStartTime = metricEndTime.minusMinutes(cwSampleDuration);

				// load the current cloudwatch metrics for the stream via the
				// metrics manager
				@SuppressWarnings("rawtypes")
				Map currentUtilisationMetrics = metricManager.queryCurrentUtilisationMetrics(cwSampleDuration,
						metricStartTime, metricEndTime);

				// process the aggregated set of Cloudwatch Datapoints
				report = processCloudwatchMetrics(currentUtilisationMetrics, metricManager.getStreamMaxCapacity(),
						cwSampleDuration, now);

				if (report != null) {
					// refresh the current max capacity after the
					// modification
					metricManager.loadMaxCapacity();
					lastShardCapacityRefreshTime = now;

					// notify all report listeners that we've completed a
					// scaling operation
					if (this.config.getScalingOperationReportListener() != null) {
						this.config.getScalingOperationReportListener().onReport(report);
					}

					if (report.getScaleDirection() != ScaleDirection.NONE) {
						LOG.info(report.toString());
					}
					report = null;
				}

				// refresh shard stats every configured period, in case someone
				// has manually updated the number of shards manually
				if (now.minusMinutes(this.config.getRefreshShardsNumberAfterMin())
						.isAfter(lastShardCapacityRefreshTime)) {
					metricManager.loadMaxCapacity();
					lastShardCapacityRefreshTime = now;
				}

				try {
					LOG.info(String.format("Next Check Cycle in %s seconds", this.config.getCheckInterval()));
					Thread.sleep(this.config.getCheckInterval() * 1000);
				} catch (InterruptedException e) {
					LOG.error(e.getMessage(), e);
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
