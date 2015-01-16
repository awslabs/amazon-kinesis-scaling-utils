/**
 * Amazon Kinesis Aggregators Copyright 2014, Amazon.com, Inc. or its
 * affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the
 * License. A copy of the License is located at http://aws.amazon.com/asl/ or in
 * the "license" file accompanying this file. This file is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.kinesis.scaling.auto;

import java.util.ArrayList;
import java.util.List;
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

    /* configuration constants for the limits on kinesis shards */
    public static final int WRITE_BYTES_PER_SHARD = 1_048_576;

    public static final int READ_BYTES_PER_SHARD = 2_097_152;

    public static final int CLOUDWATCH_PERIOD = 60;

    public static final int REFRESH_SHARD_CAPACITY_MINS = 3;

    private AutoscalingConfiguration config;

    private boolean keepRunning = true;

    private DateTime lastScaleDown = null;

    private enum ScaleDirection {
        UP, DOWN;
    }

    private StreamScaler scaler = null;

    private Exception exception;

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
    private int getStreamBytesMax() throws Exception {
        LOG.debug(String.format("Refreshing Stream %s Throughput Information",
                this.config.getStreamName()));
        Integer openShards = StreamScalingUtils.getOpenShardCount(this.kinesisClient,
                this.config.getStreamName());
        int shardMaxBytes;

        if (config.getScaleOnOperation().startsWith("PutRecord")) {
            shardMaxBytes = WRITE_BYTES_PER_SHARD;
        } else {
            shardMaxBytes = READ_BYTES_PER_SHARD;
        }

        int maxBytes = openShards.intValue() * shardMaxBytes;
        LOG.info(String.format("Stream Capacity %s Open Shards, %,d Bytes/Second", openShards,
                maxBytes));
        return maxBytes;
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
        double currentBytesMax = -1D;

        // configure cloudwatch to determine the current stream metrics
        // add the stream name dimension
        List<String> fetchMetrics = new ArrayList<>();
        List<GetMetricStatisticsRequest> cwRequests = new ArrayList<>();

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
        String metricName;
        if (this.config.getScaleOnOperation().equals("PUT")) {
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

            cwRequests.add(cwRequest);
        }

        try {
            ScalingOperationReport report = null;

            do {
                ScaleDirection scaleDirection = null;
                DateTime now = new DateTime(System.currentTimeMillis());
                DateTime metricEndTime = new DateTime(System.currentTimeMillis());

                // fetch only the last N minutes metrics
                DateTime metricStartTime = metricEndTime.minusMinutes(cwSampleDuration);

                int lowSamples = 0;
                int highSamples = 0;
                double latestPct = 0d;
                double latestBytes = 0d;

                // iterate through all the requested CloudWatch metrics (either
                // a single GetRecords, or two: PutRecord and PutRecords
                for (GetMetricStatisticsRequest req : cwRequests) {
                    req.withStartTime(metricStartTime.toDate()).withEndTime(metricEndTime.toDate());

                    // call cloudwatch to get the required metrics
                    LOG.debug(String.format(
                            "Requesting %s minutes of CloudWatch Data for Stream Metric %s",
                            cwSampleDuration, req.getMetricName()));
                    GetMetricStatisticsResult cloudWatchMetrics = cloudWatchClient.getMetricStatistics(req);

                    // if we got nothing back, then there are no puts or gets
                    // happening, so this is a full 'low sample'
                    if (cloudWatchMetrics.getDatapoints().size() == 0) {
                        lowSamples = config.getScaleDown().getScaleAfterMins();
                        currentBytesMax = 0;
                    }

                    // update the map tracking the low and high values observed
                    // for
                    // the measure
                    DateTime lastTime = null;
                    int retrievedCwSamples = 0;

                    for (Datapoint d : cloudWatchMetrics.getDatapoints()) {
                        retrievedCwSamples++;
                        currentBytesMax = (d.getSum() / CLOUDWATCH_PERIOD);
                        double currentPct = currentBytesMax / streamBytesMax;

                        if (lastTime == null || new DateTime(d.getTimestamp()).isAfter(lastTime)) {
                            latestPct = currentPct;
                            latestBytes = currentBytesMax;
                        }
                        lastTime = new DateTime(d.getTimestamp());

                        if (currentPct > new Double(this.config.getScaleUp().getScaleThresholdPct()) / 100) {
                            LOG.debug("Cached High Alarm Condition for " + currentBytesMax
                                    + " bytes (" + currentPct + "%)");
                            highSamples++;
                        } else if (currentPct < new Double(
                                this.config.getScaleDown().getScaleThresholdPct()) / 100) {
                            LOG.debug("Cached Low Alarm Condition for " + currentBytesMax
                                    + " bytes (" + currentPct + "%)");
                            lowSamples++;
                        }
                    }

                    // add low samples for the periods which we didn't get any
                    // data
                    // points, if there are any
                    if (retrievedCwSamples < cwSampleDuration) {
                        lowSamples += cwSampleDuration - retrievedCwSamples;
                    }
                }

                LOG.info(String.format("Stream Used Capacity %.2f%% (%,.0f Bytes)",
                        latestPct * 100, latestBytes));

                // check how many samples we have in the last period, and
                // flag
                // the appropriate action
                if (highSamples >= config.getScaleUp().getScaleAfterMins()) {
                    scaleDirection = ScaleDirection.UP;
                } else if (lowSamples >= config.getScaleDown().getScaleAfterMins()) {
                    scaleDirection = ScaleDirection.DOWN;
                }

                LOG.debug("Currently tracking " + highSamples + " Scale Up Alarms, and "
                        + lowSamples + " Scale Down Alarms");

                // if the metric stats indicate a scale up or down, then do the
                // action
                if (scaleDirection != null && scaleDirection.equals(ScaleDirection.UP)) {
                    // submit a scale up task
                    LOG.info(String.format(
                            "Scale Up Stream %s by %s as %s has been above %s%% for %s Minutes",
                            config.getStreamName(), scaleUpByLabel, config.getScaleOnOperation(),
                            config.getScaleUp().getScaleThresholdPct(),
                            config.getScaleUp().getScaleAfterMins()));

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
                            && now.minusMinutes(config.getScaleDown().getCoolOffMins()).isBefore(
                                    lastScaleDown)) {
                        LOG.info(String.format(
                                "Deferring Scale Down until Cool Off Period of %s Minutes has elapsed",
                                config.getScaleDown().getCoolOffMins()));
                    } else if ((this.config.getScaleOnOperation().equals("PUT") && streamBytesMax == WRITE_BYTES_PER_SHARD)
                            || (this.config.getScaleOnOperation().equals("GET") && streamBytesMax == READ_BYTES_PER_SHARD)) {
                        // do nothing - we're already at 1 shard
                        LOG.debug("Not Scaling Down - Already at Minimum of 1 Shard");
                    } else {
                        // submit a scale down
                        LOG.info(String.format(
                                "Scale Down Stream %s by %s as %s has been below %s%% for %s Minutes",
                                config.getStreamName(), scaleDownByLabel,
                                config.getScaleOnOperation(),
                                config.getScaleDown().getScaleThresholdPct(),
                                config.getScaleDown().getScaleAfterMins()));
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

                if (scaleDirection != null) {
                    // refresh the current max capacity after the
                    // modification
                    streamBytesMax = getStreamBytesMax();
                    lastShardCapacityRefreshTime = now;
                    scaleDirection = null;
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
}
