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
import com.amazonaws.services.kinesis.scaling.ScalingOperationReport;
import com.amazonaws.services.kinesis.scaling.StreamScaler;
import com.amazonaws.services.kinesis.scaling.StreamScalingUtils;

public class StreamMonitor implements Runnable {
    private final Log LOG = LogFactory.getLog(StreamMonitor.class);

    private AmazonKinesisClient kinesisClient;

    private AmazonCloudWatch cloudWatchClient;

    public static final int TIMEOUT_SECONDS = 45;

    public static final int CLOUDWATCH_PERIOD = 60;

    public static final int REFRESH_SHARD_CAPACITY_MINS = 3;

    private AutoscalingConfiguration config;

    @SuppressWarnings("unused")
    private boolean keepRunning = true;

    private DateTime lastScaleDown = null;

    private enum ScaleDirection {
        UP, DOWN;
    }

    private StreamScaler scaler = null;

    private Exception exception;

    /* incomplete constructor only for testing */
    protected StreamMonitor(AutoscalingConfiguration config, StreamScaler scaler) throws Exception {
        this.config = config;
        this.scaler = scaler;
    }

    public StreamMonitor(AutoscalingConfiguration config, ExecutorService executor)
            throws Exception {
        this.config = config;
        this.scaler = new StreamScaler(Region.getRegion(Regions.fromName(this.config.getRegion())));
        this.cloudWatchClient = new AmazonCloudWatchClient(new DefaultAWSCredentialsProviderChain());
        this.cloudWatchClient.setRegion(Region.getRegion(Regions.fromName(this.config.getRegion())));

        this.kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        this.kinesisClient.setRegion(Region.getRegion(Regions.fromName(this.config.getRegion())));
    }

    public void stop() {
        this.keepRunning = false;
        LOG.info(String.format("Signalling Monitor for Stream %s to Stop", config.getStreamName()));
    }

    /**
     * method which returns the current max capacity of a Stream based on
     * configuration of alarms on PUTS or GETS
     */
    protected int getStreamBytesMax() throws Exception {
        LOG.debug(String.format("Refreshing Stream %s Throughput Information",
                this.config.getStreamName()));
        Integer openShards = StreamScalingUtils.getOpenShardCount(this.kinesisClient,
                this.config.getStreamName());

        int maxBytes = openShards.intValue() * config.getScaleOnOperation().getMaxBytes();
        LOG.debug(String.format("Stream Capacity %s Open Shards, %,d Bytes/Second", openShards,
                maxBytes));
        return maxBytes;
    }

    /*
     * generate a single cloudwatch request for GET operations, as this is
     * instrumented as a single GetRecords metric, or create two for PUT as this
     * is instrumented as both PutRecord and PutRecords
     */
    private List<GetMetricStatisticsRequest> getCloudwatchRequests(
            KinesisOperationType operationType) {
        List<GetMetricStatisticsRequest> reqs = new ArrayList<>();
        // configure cloudwatch to determine the current stream metrics
        // add the stream name dimension
        List<String> fetchMetrics = new ArrayList<>();

        if (this.config.getScaleOnOperation().equals(KinesisOperationType.PUT)) {
            fetchMetrics.add("PutRecord.Bytes");
            fetchMetrics.add("PutRecords.Bytes");
        } else {
            fetchMetrics.add("GetRecords.Bytes");
        }

        for (String s : fetchMetrics) {
            GetMetricStatisticsRequest cwRequest = new GetMetricStatisticsRequest();

            cwRequest.withNamespace("AWS/Kinesis").withDimensions(
                    new Dimension().withName("StreamName").withValue(this.config.getStreamName())).withPeriod(
                    CLOUDWATCH_PERIOD).withStatistics(Statistic.Sum);

            cwRequest.withMetricName(s);

            reqs.add(cwRequest);
        }

        return reqs;
    }

    /* method has bee lifted out of run() for unit testing purposes */
    protected ScalingOperationReport processCloudwatchMetrics(Map<Datapoint, Double> metrics,
            double streamBytesMax, int cwSampleDuration, String scaleUpByLabel,
            String scaleDownByLabel, DateTime now) throws Exception {
        ScalingOperationReport report = null;
        double currentBytesMax = 0D;
        double currentPct = 0D;
        int lowSamples = 0;
        int highSamples = 0;
        double latestPct = 0d;
        double latestBytes = 0d;
        DateTime lastTime = null;
        ScaleDirection scaleDirection = null;

        // if we got nothing back, then there are no puts or gets
        // happening, so this is a full 'low sample'
        if (metrics.size() == 0) {
            lowSamples = this.config.getScaleDown().getScaleAfterMins();
        }

        // process the data point aggregates retrieved from CloudWatch
        // and log scale up/down votes by period
        for (Datapoint d : metrics.keySet()) {
            currentBytesMax = metrics.get(d);
            currentPct = currentBytesMax / streamBytesMax;
            

            // keep track of the last measures
            if (lastTime == null || new DateTime(d.getTimestamp()).isAfter(lastTime)) {
                latestPct = currentPct;
                latestBytes = currentBytesMax;
            }
            lastTime = new DateTime(d.getTimestamp());

            // if the pct for the datapoint exceeds or is below the
            // thresholds, then add low/high samples
            if (currentPct > new Double(this.config.getScaleUp().getScaleThresholdPct()) / 100) {
                LOG.debug("Cached High Alarm Condition for " + currentBytesMax + " bytes ("
                        + currentPct + "%)");
                highSamples++;
            } else if (currentPct < new Double(this.config.getScaleDown().getScaleThresholdPct()) / 100) {
                LOG.debug("Cached Low Alarm Condition for " + currentBytesMax + " bytes ("
                        + currentPct + "%)");
                lowSamples++;
            }
        }

        // add low samples for the periods which we didn't get any
        // data points, if there are any
        if (metrics.size() < cwSampleDuration) {
            lowSamples += cwSampleDuration - metrics.size();
        }

        LOG.info(String.format("Stream Used Capacity %.2f%% (%,.0f Bytes of %,.0f)",
                latestPct * 100, latestBytes, streamBytesMax));

        // check how many samples we have in the last period, and
        // flag the appropriate action
        if (highSamples >= config.getScaleUp().getScaleAfterMins()) {
            scaleDirection = ScaleDirection.UP;
        } else if (lowSamples >= config.getScaleDown().getScaleAfterMins()) {
            scaleDirection = ScaleDirection.DOWN;
        }

        LOG.debug("Currently tracking " + highSamples + " Scale Up Alarms, and " + lowSamples
                + " Scale Down Alarms");

        // if the metric stats indicate a scale up or down, then do the
        // action
        if (scaleDirection != null && scaleDirection.equals(ScaleDirection.UP)) {
            // submit a scale up task
            LOG.info(String.format(
                    "Scale Up Stream %s by %s as %s has been above %s%% for %s Minutes",
                    this.config.getStreamName(), scaleUpByLabel, this.config.getScaleOnOperation(),
                    this.config.getScaleUp().getScaleThresholdPct(),
                    this.config.getScaleUp().getScaleAfterMins()));

            if (this.config.getScaleUp().getScaleCount() != null) {
                report = this.scaler.scaleUp(this.config.getStreamName(),
                        this.config.getScaleUp().getScaleCount());
            } else {
                report = this.scaler.scaleUp(this.config.getStreamName(), new Double(
                        this.config.getScaleUp().getScalePct()) / 100);
            }
        } else if (scaleDirection != null && scaleDirection.equals(ScaleDirection.DOWN)) {
            // check the cool down interval
            if (lastScaleDown != null
                    && now.minusMinutes(this.config.getScaleDown().getCoolOffMins()).isBefore(
                            lastScaleDown)) {
                LOG.info(String.format(
                        "Deferring Scale Down until Cool Off Period of %s Minutes has elapsed",
                        this.config.getScaleDown().getCoolOffMins()));
            } else if (streamBytesMax == this.config.getScaleOnOperation().getMaxBytes()) {
                // do nothing - we're already at 1 shard
                LOG.debug("Not Scaling Down - Already at Minimum of 1 Shard");
            } else {
                // submit a scale down
                LOG.info(String.format(
                        "Scale Down Stream %s by %s as %s has been below %s%% for %s Minutes",
                        this.config.getStreamName(), scaleDownByLabel,
                        config.getScaleOnOperation(),
                        this.config.getScaleDown().getScaleThresholdPct(),
                        this.config.getScaleDown().getScaleAfterMins()));
                if (this.config.getScaleDown().getScaleCount() != null) {
                    report = this.scaler.scaleDown(this.config.getStreamName(),
                            this.config.getScaleDown().getScaleCount());
                } else {
                    report = this.scaler.scaleDown(this.config.getStreamName(), new Double(
                            this.config.getScaleDown().getScalePct()) / 100);
                }

                lastScaleDown = new DateTime(System.currentTimeMillis());
            }
        } else {
            // scale direction not set, so we're not going to scale
            // up or down - everything fine
            LOG.debug("No Scaling Directive received");
        }

        return report;
    }

    @Override
    public void run() {
        LOG.info(String.format("Started Stream Monitor for %s", config.getStreamName()));
        DateTime lastShardCapacityRefreshTime = new DateTime(System.currentTimeMillis());

        // determine shard capacity on the metric we will scale on
        int streamBytesMax;
        try {
            streamBytesMax = getStreamBytesMax();
        } catch (Exception e) {
            this.exception = e;
            return;
        }

        // configure log labels
        String scaleUpByLabel = "";
        String scaleDownByLabel = "";
        if (this.config.getScaleUp().getScaleCount() != null) {
            scaleUpByLabel = String.format("%s Shards", this.config.getScaleUp().getScaleCount());
        } else {
            scaleUpByLabel = String.format("%s%%", this.config.getScaleUp().getScalePct());
        }
        if (this.config.getScaleDown().getScaleCount() != null) {
            scaleDownByLabel = String.format("%s Shards",
                    this.config.getScaleDown().getScaleCount());
        } else {
            scaleDownByLabel = String.format("%s%%", this.config.getScaleDown().getScalePct());
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

                Map<Datapoint, Double> metrics = new HashMap<>();

                // iterate through all the requested CloudWatch metrics (either
                // a single GetRecords, or two: PutRecord and PutRecords and
                // collapse them down into a single map of sum bytes indexed by
                // datapoint
                //
                // TODO Figure out how to mock this bit
                for (GetMetricStatisticsRequest req : cwRequests) {
                    double sampleBytes = 0D;

                    req.withStartTime(metricStartTime.toDate()).withEndTime(metricEndTime.toDate());

                    // call cloudwatch to get the required metrics
                    LOG.debug(String.format(
                            "Requesting %s minutes of CloudWatch Data for Stream Metric %s",
                            cwSampleDuration, req.getMetricName()));
                    GetMetricStatisticsResult cloudWatchMetrics = cloudWatchClient.getMetricStatistics(req);

                    // aggregate the sample bytes by datapoint into a map, so
                    // that PutRecords and PutRecord measures are added together
                    for (Datapoint d : cloudWatchMetrics.getDatapoints()) {
                        if (metrics.containsKey(d)) {
                            sampleBytes = metrics.get(d);
                        } else {
                            sampleBytes = 0d;
                        }
                        sampleBytes += (d.getSum() / CLOUDWATCH_PERIOD);
                        metrics.put(d, sampleBytes);
                    }
                }

                // process the aggregated set of Cloudwatch Datapoints
                report = processCloudwatchMetrics(metrics, streamBytesMax, cwSampleDuration,
                        scaleUpByLabel, scaleDownByLabel, now);

                if (report != null) {
                    // refresh the current max capacity after the
                    // modification
                    streamBytesMax = getStreamBytesMax();
                    lastShardCapacityRefreshTime = now;
                }

                if (report != null) {
                    LOG.info(report.toString());
                    report = null;
                }

                // refresh shard stats every configured period, in case someone
                // has manually updated the number of shards
                if (now.minusMinutes(REFRESH_SHARD_CAPACITY_MINS).isAfter(
                        lastShardCapacityRefreshTime)) {
                    streamBytesMax = getStreamBytesMax();
                    lastShardCapacityRefreshTime = now;
                }

                try {
                    LOG.debug("Sleep");
                    Thread.sleep(TIMEOUT_SECONDS * 1000);
                } catch (InterruptedException e) {
                    LOG.error(e);
                    break;
                }
            } while (keepRunning = true);

            LOG.info(String.format("Stream Monitor for %s in %s Completed. Exiting.",
                    this.config.getStreamName(), this.config.getRegion()));
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
}
