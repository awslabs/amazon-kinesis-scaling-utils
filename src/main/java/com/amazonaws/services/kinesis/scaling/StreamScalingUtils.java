/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.scaling.StreamScaler.SortOrder;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.MergeShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.SplitShardRequest;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

public class StreamScalingUtils {
	private static final Logger LOG = LoggerFactory.getLogger(StreamScalingUtils.class);

	public static final int DESCRIBE_RETRIES = 10;

	public static final int MODIFY_RETRIES = 10;

	// retry timeout set to 100ms as API's will potentially throttle > 10/sec
	public static final int RETRY_TIMEOUT_MS = 100;

	// rounding scale for BigInteger and BigDecimal comparisons
	public static final int PCT_COMPARISON_SCALE = 10;

	public static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_DOWN;

	private static interface KinesisOperation {
		public Object run(KinesisClient client);
	}

	/**
	 * Method to do a fuzzy comparison between two doubles, so that we can make
	 * generalisations about allocation of keyspace to shards. For example, when we
	 * have a stream of 3 shards, we'll have shards of 33, 33, and 34% of the
	 * keyspace - these must all be treated as equal
	 *
	 * @param a
	 * @param b
	 * @return
	 */
	public static int softCompare(double a, double b) {
		// allow variation by 1 order of magnitude greater than the comparison
		// scale
		final BigDecimal acceptedVariation = BigDecimal.valueOf(1d)
				.divide(BigDecimal.valueOf(10d).pow(PCT_COMPARISON_SCALE - 1));

		BigDecimal first = new BigDecimal(a).setScale(PCT_COMPARISON_SCALE, ROUNDING_MODE);
		BigDecimal second = new BigDecimal(b).setScale(PCT_COMPARISON_SCALE, ROUNDING_MODE);

		BigDecimal variation = first.subtract(second).abs();

		// if the variation of the two values is within the accepted variation,
		// then we return 'equal'
		if (variation.compareTo(acceptedVariation) < 0) {
			return 0;
		} else {
			return first.compareTo(second);
		}
	}

	/**
	 * Wait for a Stream to become available or transition to the indicated status
	 *
	 * @param streamName
	 * @param status
	 * @throws Exception
	 */
	public static void waitForStreamStatus(KinesisClient kinesisClient, String streamName, String status)
			throws Exception {
		boolean ok = false;
		String streamStatus;
		// stream mutation takes around 30 seconds, so we'll start with 20 as
		// a timeout
		int waitTimeout = 20000;
		do {
			streamStatus = getStreamStatus(kinesisClient, streamName);
			if (!streamStatus.equals(status)) {
				Thread.sleep(waitTimeout);
				// reduce the wait timeout from the initial wait time
				waitTimeout = 1000;
			} else {
				ok = true;
			}
		} while (!ok);
	}

	/**
	 * Get the status of a Stream
	 *
	 * @param streamName
	 * @return
	 */
	protected static String getStreamStatus(KinesisClient kinesisClient, String streamName) throws Exception {
		return describeStream(kinesisClient, streamName).streamStatus().name();
	}

	public static StreamDescriptionSummary describeStream(final KinesisClient kinesisClient, final String streamName)
			throws Exception {
		KinesisOperation describe = new KinesisOperation() {
			public Object run(KinesisClient client) {
				DescribeStreamSummaryResponse result = client
						.describeStreamSummary(DescribeStreamSummaryRequest.builder().streamName(streamName).build());

				return result.streamDescriptionSummary();
			}
		};
		return (StreamDescriptionSummary) doOperation(kinesisClient, describe, streamName, DESCRIBE_RETRIES, false);
	}

	public static List<Shard> listShards(final KinesisClient kinesisClient, final String streamName,
			final String shardIdStart) throws Exception {
		LOG.debug(String.format("Listing Stream %s from Shard %s", streamName, shardIdStart));

		KinesisOperation describe = new KinesisOperation() {
			public Object run(KinesisClient client) {
				ListShardsRequest.Builder builder = ListShardsRequest.builder().streamName(streamName);
				ListShardsRequest req = null;
				boolean hasMoreResults = true;
				List<Shard> shards = new ArrayList<>();

				while (hasMoreResults) {
					if (shardIdStart != null && (req != null && req.nextToken() == null)) {
						builder.exclusiveStartShardId(shardIdStart);
					}
					ListShardsResponse result = client.listShards(builder.build());
					shards.addAll(result.shards());

					if (result.nextToken() == null) {
						hasMoreResults = false;
					} else {
						req = ListShardsRequest.builder().nextToken(result.nextToken()).build();
					}

				}
				return shards;
			}
		};
		return (List<Shard>) doOperation(kinesisClient, describe, streamName, DESCRIBE_RETRIES, false);
	}

	public static Shard getShard(final KinesisClient kinesisClient, final String streamName, final String shardIdStart)
			throws Exception {
		LOG.debug(String.format("Getting Shard %s for Stream %s", shardIdStart, streamName));

		KinesisOperation describe = new KinesisOperation() {
			public Object run(KinesisClient client) {
				// reduce the shardIdStart by 1 as the API uses it as an exclusive start key not
				// a filter
				String shardIdToQuery = new BigDecimal(shardIdStart).subtract(new BigDecimal("1")).toString();
				ListShardsRequest req = ListShardsRequest.builder().streamName(streamName)
						.exclusiveStartShardId(shardIdToQuery).build();
				ListShardsResponse result = client.listShards(req);

				return result.shards().get(0);
			}
		};
		return (Shard) doOperation(kinesisClient, describe, streamName, DESCRIBE_RETRIES, false);
	}

	public static void splitShard(final KinesisClient kinesisClient, final String streamName, final String shardId,
			final BigInteger targetHash, final boolean waitForActive) throws Exception {
		LOG.debug(String.format("Splitting Shard %s at %s", shardId, targetHash.toString()));

		KinesisOperation split = new KinesisOperation() {
			public Object run(KinesisClient client) {
				final SplitShardRequest req = SplitShardRequest.builder().streamName(streamName).shardToSplit(shardId)
						.newStartingHashKey(targetHash.toString()).build();
				client.splitShard(req);

				return null;
			}
		};
		doOperation(kinesisClient, split, streamName, MODIFY_RETRIES, waitForActive);
	}

	public static void mergeShards(final KinesisClient kinesisClient, final String streamName,
			final ShardHashInfo lowerShard, final ShardHashInfo higherShard, final boolean waitForActive)
			throws Exception {
		LOG.debug(String.format("Merging Shard %s and %s", lowerShard, higherShard));

		KinesisOperation merge = new KinesisOperation() {
			public Object run(KinesisClient client) {
				final MergeShardsRequest req = MergeShardsRequest.builder().streamName(streamName)
						.shardToMerge(lowerShard.getShardId()).adjacentShardToMerge(higherShard.getShardId()).build();
				client.mergeShards(req);

				return null;
			}
		};
		doOperation(kinesisClient, merge, streamName, MODIFY_RETRIES, waitForActive);
	}

	private static Object doOperation(KinesisClient kinesisClient, KinesisOperation operation, String streamName,
			int retries, boolean waitForActive) throws Exception {
		boolean done = false;
		int attempts = 0;
		Object result = null;
		do {
			attempts++;
			try {
				result = operation.run(kinesisClient);

				if (waitForActive) {
					waitForStreamStatus(kinesisClient, streamName, "ACTIVE");
				}
				done = true;
			} catch (ResourceInUseException e) {
				// thrown when the Shard is mutating - wait until we are able to
				// do the modification or ResourceNotFoundException is thrown
				Thread.sleep(1000);
			} catch (LimitExceededException lee) {
				// API Throttling
				LOG.warn(String.format("LimitExceededException for Stream %s", streamName));

				Thread.sleep(getTimeoutDuration(attempts));
			}
		} while (!done && attempts < retries);

		if (!done) {
			throw new Exception(String.format("Unable to Complete Kinesis Operation after %s Retries", retries));
		} else {
			return result;
		}
	}

	// calculate an exponential backoff based on the attempt count
	private static final long getTimeoutDuration(int attemptCount) {
		return new Double(Math.pow(2, attemptCount) * RETRY_TIMEOUT_MS).longValue();
	}

	private static final int compareShardsByStartHash(Shard o1, Shard o2) {
		return new BigInteger(o1.hashKeyRange().startingHashKey())
				.compareTo(new BigInteger(o2.hashKeyRange().startingHashKey()));
	}

	public static int getOpenShardCount(KinesisClient kinesisClient, String streamName) throws Exception {
		return StreamScalingUtils.describeStream(kinesisClient, streamName).openShardCount();
	}

	/**
	 * Get a list of all Open shards ordered by their start hash
	 *
	 * @param streamName
	 * @return A Map of only Open Shards indexed by the Shard ID
	 */
	public static Map<String, ShardHashInfo> getOpenShards(KinesisClient kinesisClient, String streamName,
			String lastShardId) throws Exception {
		return getOpenShards(kinesisClient, streamName, SortOrder.ASCENDING, lastShardId);
	}

	public static ShardHashInfo getOpenShard(KinesisClient kinesisClient, String streamName, String shardId)
			throws Exception {
		Shard s = getShard(kinesisClient, streamName, shardId);

		if (!s.shardId().equals(shardId)) {
			throw new Exception(String.format("Shard %s not found in Stream %s", shardId, streamName));
		} else {
			return new ShardHashInfo(streamName, s);
		}
	}

	public static Map<String, ShardHashInfo> getOpenShards(KinesisClient kinesisClient, String streamName,
			SortOrder sortOrder, String lastShardId) throws Exception {
		Collection<String> openShardNames = new ArrayList<>();
		Map<String, ShardHashInfo> shardMap = new LinkedHashMap<>();

		// load all the open shards on the Stream and sort if required
		for (Shard shard : listShards(kinesisClient, streamName, lastShardId)) {
			openShardNames.add(shard.shardId());
			shardMap.put(shard.shardId(), new ShardHashInfo(streamName, shard));

			// remove this Shard's parents from the set of active shards - they
			// are now closed and cannot be modified or written to
			if (shard.parentShardId() != null) {
				openShardNames.remove(shard.parentShardId());
				shardMap.remove(shard.parentShardId());
			}
			if (shard.adjacentParentShardId() != null) {
				openShardNames.remove(shard.adjacentParentShardId());
				shardMap.remove(shard.adjacentParentShardId());
			}
		}

		// create a List of Open shards for sorting
		List<Shard> sortShards = new ArrayList<>();
		for (String s : openShardNames) {
			// paranoid null check in case we get a null map entry
			if (s != null) {
				sortShards.add(shardMap.get(s).getShard());
			}
		}

		if (sortOrder.equals(SortOrder.ASCENDING)) {
			// sort the list into lowest start hash order
			Collections.sort(sortShards, new Comparator<Shard>() {
				public int compare(Shard o1, Shard o2) {
					return compareShardsByStartHash(o1, o2);
				}
			});
		} else if (sortOrder.equals(SortOrder.DESCENDING)) {
			// sort the list into highest start hash order
			Collections.sort(sortShards, new Comparator<Shard>() {
				public int compare(Shard o1, Shard o2) {
					return compareShardsByStartHash(o1, o2) * -1;
				}
			});
		} // else we were supplied a NONE sort order so no sorting

		// build the Shard map into the correct order
		shardMap.clear();
		for (Shard s : sortShards) {
			shardMap.put(s.shardId(), new ShardHashInfo(streamName, s));
		}

		return shardMap;
	}

	public static void sendNotification(SnsClient snsClient, String notificationARN, String subject, String message) {
		final PublishRequest req = PublishRequest.builder().topicArn(notificationARN).message(message).subject(subject)
				.build();
		snsClient.publish(req);
	}
}
