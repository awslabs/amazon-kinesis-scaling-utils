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
package com.amazonaws.services.kinesis.scaling;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.scaling.StreamScaler.SortOrder;

public class StreamScalingUtils {
	public static final int DESCRIBE_RETRIES = 10;

	public static final int MODIFY_RETRIES = 10;

	// retry timeout set to 100ms as API's will potentially throttle > 10/sec
	public static final int RETRY_TIMEOUT_MS = 100;

	// rounding scale for BigInteger and BigDecimal comparisons
	public static final int PCT_COMPARISON_SCALE = 10;

	public static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_DOWN;

	private static interface KinesisOperation {
		public Object run(AmazonKinesis client);
	}

	/**
	 * Method to do a fuzzy comparison between two doubles, so that we can make
	 * generalisations about allocation of keyspace to shards. For example, when
	 * we have a stream of 3 shards, we'll have shards of 33, 33, and 34% of the
	 * keyspace - these must all be treated as equal
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static int softCompare(double a, double b) {
		// allow variation by 1 order of magnitude greater than the comparison
		// scale
		final BigDecimal acceptedVariation = BigDecimal.valueOf(1d).divide(
				BigDecimal.valueOf(10d).pow(PCT_COMPARISON_SCALE - 1));

		BigDecimal first = new BigDecimal(a).setScale(PCT_COMPARISON_SCALE,
				ROUNDING_MODE);
		BigDecimal second = new BigDecimal(b).setScale(PCT_COMPARISON_SCALE,
				ROUNDING_MODE);

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
	 * Wait for a Stream to become available or transition to the indicated
	 * status
	 * 
	 * @param streamName
	 * @param status
	 * @throws Exception
	 */
	public static void waitForStreamStatus(AmazonKinesis kinesisClient,
			String streamName, String status) throws Exception {
		boolean ok = false;
		String streamStatus;
		do {
			streamStatus = getStreamStatus(kinesisClient, streamName);
			if (!streamStatus.equals(status)) {
				Thread.sleep(1000);
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
	protected static String getStreamStatus(AmazonKinesis kinesisClient,
			String streamName) throws Exception {
		return describeStream(kinesisClient, streamName, null)
				.getStreamDescription().getStreamStatus();
	}

	public static DescribeStreamResult describeStream(
			final AmazonKinesis kinesisClient, final String streamName,
			final String shardIdStart) throws Exception {
		KinesisOperation describe = new KinesisOperation() {
			public Object run(AmazonKinesis client) {
				DescribeStreamResult result = client
						.describeStream(new DescribeStreamRequest()
								.withStreamName(streamName)
								.withExclusiveStartShardId(shardIdStart));

				return result;
			}
		};
		return (DescribeStreamResult) doOperation(kinesisClient, describe,
				streamName, DESCRIBE_RETRIES, false);

	}

	public static void splitShard(final AmazonKinesis kinesisClient,
			final String streamName, final String shardId,
			final BigInteger targetHash, final boolean waitForActive)
			throws Exception {
		KinesisOperation split = new KinesisOperation() {
			public Object run(AmazonKinesis client) {
				client.splitShard(streamName, shardId, targetHash.toString());

				return null;
			}
		};
		doOperation(kinesisClient, split, streamName, MODIFY_RETRIES,
				waitForActive);
	}

	public static void mergeShards(final AmazonKinesis kinesisClient,
			final String streamName, final ShardHashInfo lowerShard,
			final ShardHashInfo higherShard, final boolean waitForActive)
			throws Exception {
		KinesisOperation merge = new KinesisOperation() {
			public Object run(AmazonKinesis client) {
				client.mergeShards(streamName, lowerShard.getShardId(),
						higherShard.getShardId());

				return null;
			}
		};
		doOperation(kinesisClient, merge, streamName, MODIFY_RETRIES,
				waitForActive);
	}

	private static Object doOperation(AmazonKinesis kinesisClient,
			KinesisOperation operation, String streamName, int retries,
			boolean waitForActive) throws Exception {
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
				Thread.sleep(getTimeoutDuration(attempts));
			}
		} while (!done && attempts < retries);

		if (!done) {
			throw new Exception(String.format(
					"Unable to Complete Kinesis Operation after %s Retries",
					retries));
		} else {
			return result;
		}
	}

	// calculate an exponential backoff based on the attempt count
	private static final long getTimeoutDuration(int attemptCount) {
		return new Double(Math.pow(2, attemptCount) * RETRY_TIMEOUT_MS)
				.longValue();
	}

	private static final int compareShardsByStartHash(Shard o1, Shard o2) {
		return new BigInteger(o1.getHashKeyRange().getStartingHashKey())
				.compareTo(new BigInteger(o2.getHashKeyRange()
						.getStartingHashKey()));
	}

	public static int getOpenShardCount(AmazonKinesisClient kinesisClient,
			String streamName) throws Exception {
		return getOpenShards(kinesisClient, streamName, SortOrder.NONE)
				.keySet().size();
	}

	/**
	 * Get a list of all Open shards ordered by their start hash
	 * 
	 * @param streamName
	 * @return A Map of only Open Shards indexed by the Shard ID
	 */
	public static Map<String, ShardHashInfo> getOpenShards(
			AmazonKinesisClient kinesisClient, String streamName)
			throws Exception {
		return getOpenShards(kinesisClient, streamName, SortOrder.ASCENDING);
	}

	public static ShardHashInfo getOpenShard(AmazonKinesisClient kinesisClient,
			String streamName, String shardId) throws Exception {

		ShardHashInfo s = getOpenShards(kinesisClient, streamName,
				Arrays.asList(shardId)).values().iterator().next();

		if (s == null) {
			throw new Exception(String.format(
					"Shard %s not found in Stream %s", shardId, streamName));
		} else {
			return s;
		}
	}

	public static Map<String, ShardHashInfo> getOpenShards(
			AmazonKinesisClient kinesisClient, String streamName,
			List<String> shardIds) throws Exception {
		Map<String, ShardHashInfo> openShards = getOpenShards(kinesisClient,
				streamName);
		Map<String, ShardHashInfo> out = new LinkedHashMap<>();

		for (String s : openShards.keySet()) {
			for (String t : shardIds) {
				if (s.equals(t)) {
					out.put(s, openShards.get(s));
				}
			}
		}

		if (out.size() != shardIds.size()) {
			throw new Exception("One or more requested Shards are not Open");
		}

		return out;
	}

	public static Map<String, ShardHashInfo> getOpenShards(
			AmazonKinesisClient kinesisClient, String streamName,
			SortOrder sortOrder) throws Exception {
		StreamDescription stream = null;
		Collection<String> openShardNames = new ArrayList<String>();
		Map<String, ShardHashInfo> shardMap = new LinkedHashMap<>();

		// load all shards on the stream
		List<Shard> allShards = new ArrayList<>();
		String lastShardId = null;
		do {
			stream = describeStream(kinesisClient, streamName, lastShardId)
					.getStreamDescription();
			for (Shard shard : stream.getShards()) {
				allShards.add(shard);
				lastShardId = shard.getShardId();
			}
		} while (/* in some cases the describeStream call will return nothing */stream == null
				|| stream.getShards() == null
				|| stream.getShards().size() == 0
				|| stream.getHasMoreShards());

		// load all the open shards on the Stream and sort if required
		for (Shard shard : allShards) {
			openShardNames.add(shard.getShardId());
			shardMap.put(shard.getShardId(), new ShardHashInfo(streamName,
					shard));

			// remove this Shard's parents from the set of active shards - they
			// are now closed and cannot be modified or written to
			if (shard.getParentShardId() != null) {
				openShardNames.remove(shard.getParentShardId());
				shardMap.remove(shard.getParentShardId());
			}
			if (shard.getAdjacentParentShardId() != null) {
				openShardNames.remove(shard.getAdjacentParentShardId());
				shardMap.remove(shard.getAdjacentParentShardId());
			}
		}

		// create a List of Open shards for sorting
		List<Shard> sortShards = new ArrayList<Shard>();
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
			shardMap.put(s.getShardId(), new ShardHashInfo(streamName, s));
		}

		return shardMap;
	}
}
