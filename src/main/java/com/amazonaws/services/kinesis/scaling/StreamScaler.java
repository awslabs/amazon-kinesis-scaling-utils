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

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

/**
 * Utility for scaling a Kinesis Stream. Places a priority on eventual balancing
 * of the Stream Keyspace by using a left leaning balanced tree topology. Also
 * places a priority on low impact to the Stream by making only one Shard
 * modification at any given time.
 */
public class StreamScaler {
	public enum SortOrder {
		ASCENDING, DESCENDING, NONE;
	}

	/**
	 * The supported Scaling Actions available for a Stream from this Utility
	 */
	public static enum ScalingAction {
		scaleUp, scaleDown, resize, report, split, merge;
	}

	public static enum ScaleBy {
		count, pct;
	}

	private final String AWSApplication = "KinesisScalingUtility";

	private final String version = ".9.3.5";

	private final NumberFormat pctFormat = NumberFormat.getPercentInstance();

	private static final Log LOG = LogFactory.getLog(StreamScaler.class);

	private AmazonKinesisClient kinesisClient;

	private static final Region region = Region.getRegion(Regions.US_EAST_1);

	/** No Args Constructor for scaling a Stream */
	public StreamScaler() throws Exception {
		this(region);
	}

	public StreamScaler(Region region) throws Exception {
		pctFormat.setMaximumFractionDigits(1);

		// use the default provider chain plus support for classpath
		// properties
		// files
		ClientConfiguration config = new ClientConfiguration();

		StringBuilder userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT);
		userAgent.append(" ");
		userAgent.append(this.AWSApplication);
		userAgent.append("/");
		userAgent.append(this.version);
		config.setUserAgent(userAgent.toString());

		kinesisClient = new AmazonKinesisClient(new AWSCredentialsProviderChain(
				new DefaultAWSCredentialsProviderChain(), new ClasspathPropertiesFileCredentialsProvider()), config);
		kinesisClient.setRegion(region);

		String kinesisEndpoint = System.getProperty("kinesisEndpoint");
		if (kinesisEndpoint != null) {
			kinesisClient.setEndpoint(kinesisEndpoint);
		}

		if (kinesisClient.getServiceName() == null) {
			throw new Exception("Unable to reach Kinesis Service");
		}
	}

	/**
	 * Get a references to the Kinesis Client in use
	 * 
	 * @return
	 */
	protected AmazonKinesisClient getClient() {
		return this.kinesisClient;
	}

	/**
	 * Scale up a Stream by a fixed amount of Shards
	 * 
	 * @param streamName
	 *            The Stream name to scale
	 * @param byShardCount
	 *            The number of Shards to add
	 * @return A Map of the final state of the Stream after Sharding, indexed by
	 *         Shard Name * @throws Exception
	 */
	public ScalingOperationReport scaleUp(String streamName, int byShardCount, Integer minShards, Integer maxShards)
			throws Exception {
		if (byShardCount <= 0) {
			throw new Exception("Shard Count must be a positive number");
		}

		int currentSize = StreamScalingUtils.getOpenShardCount(kinesisClient, streamName);

		return doResize(streamName, currentSize + byShardCount, minShards, maxShards);
	}

	public ScalingOperationReport scaleUp(String streamName, String shardId, int byShardCount, Integer minShards,
			Integer maxShards) throws Exception {
		int openShardCount = StreamScalingUtils.getOpenShardCount(this.kinesisClient, streamName);

		// the target percentage for this scaled up stream is the same as if all
		// shards were scaled to this level
		double simulatedTargetPct = 1d / (openShardCount * byShardCount);

		// scale this specific shard by the count requested
		return scaleStream(streamName, shardId, byShardCount, simulatedTargetPct, 0, 0, System.currentTimeMillis(),
				minShards, maxShards);
	}

	/**
	 * Scale down a Stream by a fixed number of Shards
	 * 
	 * @param streamName
	 *            The Stream name to scale
	 * @param byShardCount
	 *            The number of Shards to reduce size by
	 * @return A Map of the final state of the Stream after Sharding, indexed by
	 *         Shard Name * @throws Exception
	 */
	public ScalingOperationReport scaleDown(String streamName, int byShardCount, Integer minShards, Integer maxShards)
			throws Exception {
		if (byShardCount <= 0) {
			throw new Exception(streamName + ": Shard Count must be a positive number");
		}

		int currentSize = StreamScalingUtils.getOpenShardCount(kinesisClient, streamName);

		if (currentSize == 1) {
			throw new AlreadyOneShardException();
		}

		return doResize(streamName, Math.max(currentSize - byShardCount, 1), minShards, maxShards);
	}

	/**
	 * Scale down a Shard by a Percentage of current capacity
	 * 
	 * @param streamName
	 *            The Stream name to scale
	 * @param byPct
	 *            The Percentage by which to reduce capacity on the Stream
	 * @return A Map of the final state of the Stream after Sharding, indexed by
	 *         Shard Name * @throws Exception
	 */
	public ScalingOperationReport scaleDown(String streamName, double byPct, Integer minShards, Integer maxShards)
			throws Exception {
		double scalePct = byPct;
		if (scalePct < 0)
			throw new Exception(streamName + ": Scaling Percent should be a positive number");

		int currentSize = StreamScalingUtils.getOpenShardCount(kinesisClient, streamName);

		if (currentSize == 1) {
			throw new AlreadyOneShardException();
		}

		int newSize = Math.max(new Double(Math.ceil(currentSize - Math.max(currentSize * scalePct, 1))).intValue(), 1);

		if (newSize > 0) {
			return doResize(streamName, newSize, minShards, maxShards);
		} else {
			return null;
		}
	}

	/**
	 * Scale up a Stream by a Percentage of current capacity. Gentle reminder -
	 * growing by 100% (1) is doubling in size, growing by 200% (2) is tripling
	 * 
	 * @param streamName
	 *            The Stream name to scale
	 * @param byPct
	 *            The Percentage by which to scale up the Stream
	 * @return A Map of the final state of the Stream after Sharding, indexed by
	 *         Shard Name * @throws Exception
	 */
	public ScalingOperationReport scaleUp(String streamName, double byPct, Integer minShards, Integer maxShards)
			throws Exception {
		if (byPct < 0)
			throw new Exception(streamName + ": Scaling Percent should be a positive number");
		int currentSize = StreamScalingUtils.getOpenShardCount(kinesisClient, streamName);

		int newSize = new Double(Math.ceil(currentSize + (currentSize * byPct))).intValue();

		if (newSize > 0) {
			return doResize(streamName, newSize, minShards, maxShards);
		} else {
			return null;
		}
	}

	/**
	 * Resize a Stream to the indicated number of Shards
	 * 
	 * @param streamName
	 *            The Stream name to scale
	 * @param targetShardCount
	 *            The desired number of shards
	 * @return A Map of the final state of the Stream after Sharding, indexed by
	 *         Shard Name
	 * @throws Exception
	 */
	public ScalingOperationReport resize(String streamName, int targetShardCount, Integer minShards, Integer maxShards)
			throws Exception {
		return doResize(streamName, targetShardCount, minShards, maxShards);
	}

	public String report(String streamName) throws Exception {
		return new ScalingOperationReport(StreamScalingUtils.getOpenShards(kinesisClient, streamName, (String) null))
				.toString();
	}

	public ScalingOperationReport reportFor(String streamName, int operationsMade, ScaleDirection scaleDirection)
			throws Exception {
		return new ScalingOperationReport(StreamScalingUtils.getOpenShards(kinesisClient, streamName, (String) null),
				operationsMade, scaleDirection);
	}

	private ScalingOperationReport doResize(String streamName, int targetShardCount, Integer minShards,
			Integer maxShards) throws Exception {
		if (!(targetShardCount > 0)) {
			throw new Exception(streamName + ": Cannot resize to 0 or negative Shard Count");
		}
		int operationsMade = 0;
		final int currentShards = StreamScalingUtils.getOpenShardCount(kinesisClient, streamName);
		final double pct = 1d / targetShardCount;
		int shardsCompleted = 0;
		final long startTime = System.currentTimeMillis();

		return scaleStream(streamName, currentShards, targetShardCount, pct, operationsMade, shardsCompleted, startTime,
				minShards, maxShards);
	}

	private void reportProgress(String streamName, int shardsCompleted, int currentCount, int shardsRemaining,
			long startTime) {
		int shardsTotal = shardsCompleted + shardsRemaining;
		double pctComplete = new Double(shardsCompleted) / new Double(shardsTotal);
		double estRemaining = (((System.currentTimeMillis() - startTime) / 1000) / pctComplete);
		LOG.info(String.format(
				"%s: Shard Modification %s Complete, (%s Pending, %s Completed). Current Size %s Shards with Approx %s Seconds Remaining",
				streamName, pctFormat.format(pctComplete), shardsRemaining, shardsCompleted, currentCount,
				new Double(estRemaining).intValue()));
	}

	private Stack<ShardHashInfo> getOpenShardStack(String streamName) throws Exception {
		// populate the stack with the current set of shards
		Stack<ShardHashInfo> shardStack = new Stack<>();
		List<ShardHashInfo> shards = new ArrayList<>(
				StreamScalingUtils.getOpenShards(kinesisClient, streamName, SortOrder.DESCENDING, null).values());
		for (ShardHashInfo s : shards) {
			shardStack.push(s);
		}

		return shardStack;
	}

	private ScalingOperationReport scaleStream(String streamName, String shardId, int targetShards, double targetPct,
			int operationsMade, int shardsCompleted, long startTime, Integer minShards, Integer maxShards)
					throws Exception {
		Stack<ShardHashInfo> shardStack = new Stack<>();
		shardStack.add(StreamScalingUtils.getOpenShard(this.kinesisClient, streamName, shardId));

		LOG.info(String.format("Scaling Shard %s:%s into %s Shards (Keyspace Share %.0f%%)", streamName, shardId,
				targetShards, targetPct * 100));

		return scaleStream(streamName, 1, targetShards, targetPct, operationsMade, shardsCompleted, startTime,
				shardStack, minShards, maxShards);

	}

	private ScalingOperationReport scaleStream(String streamName, int originalShardCount, int targetShards,
			double targetPct, int operationsMade, int shardsCompleted, long startTime, Stack<ShardHashInfo> shardStack,
			Integer minCount, Integer maxCount) throws Exception {
		boolean checkMinMax = minCount != null || maxCount != null;
		String lastShardLower = null;
		String lastShardHigher = null;
		ScaleDirection scaleDirection = originalShardCount >= targetShards ? ScaleDirection.DOWN : ScaleDirection.UP;

		// seed the current shard count from the working stack
		int currentCount = shardStack.size();

		// we'll run iteratively until the shard stack is emptied or we reach
		// one of the caps
		do {
			if (checkMinMax) {
				// stop scaling if we've reached the min or max count
				boolean stopOnCap = false;
				String message = null;
				if (minCount != null && currentCount == minCount && targetShards <= minCount) {
					stopOnCap = true;
					message = String.format("%s: Minimum Shard Count of %s Reached", streamName, minCount);
				}
				if (maxCount != null && currentCount == maxCount && targetShards >= maxCount) {
					message = String.format("%s: Maximum Shard Count of %s Reached", streamName, maxCount);
					stopOnCap = true;
				}
				if (stopOnCap) {
					LOG.info(message);
					return reportFor(streamName, operationsMade, scaleDirection);
				}
			}

			// report progress every shard completed
			if (shardsCompleted > 0) {
				reportProgress(streamName, shardsCompleted, currentCount, shardStack.size(), startTime);
			}

			// once the stack is emptied, return a report of the hash space
			// allocation
			if (shardStack.empty()) {
				return reportFor(streamName, operationsMade, scaleDirection);
			}

			ShardHashInfo lowerShard = shardStack.pop();
			if (lowerShard != null) {
				lastShardLower = lowerShard.getShardId();
			} else {
				throw new Exception(String.format("%s: Null ShardHashInfo retrieved after processing %s", streamName,
						lastShardLower));
			}

			// first check is if the bottom shard is smaller or larger than our
			// target width
			if (StreamScalingUtils.softCompare(lowerShard.getPctWidth(), targetPct) < 0) {
				if (shardStack.empty()) {
					// our current shard is smaller than the target size, but
					// there's nothing else to do
					return reportFor(streamName, operationsMade, scaleDirection);
				} else {
					// get the next higher shard
					ShardHashInfo higherShard = shardStack.pop();

					if (higherShard != null) {
						lastShardHigher = higherShard.getShardId();
					}

					if (StreamScalingUtils.softCompare(lowerShard.getPctWidth() + higherShard.getPctWidth(),
							targetPct) > 0) {
						// The two lowest shards together are larger than the
						// target size, so split the upper at the target offset
						// and
						// merge the lower of the two new shards to the lowest
						// shard
						AdjacentShards splitUpper = higherShard.doSplit(kinesisClient,
								targetPct - lowerShard.getPctWidth(), shardStack.isEmpty() ? higherShard.getShardId()
										: shardStack.lastElement().getShardId());
						operationsMade++;

						// place the upper of the two new shards onto the stack
						shardStack.push(splitUpper.getHigherShard());

						// merge lower of the new shards with the lowest shard
						LOG.debug(String.format("Merging Shard %s with %s", lowerShard.getShardId(),
								splitUpper.getLowerShard().getShardId()));
						ShardHashInfo lowerMerged = new AdjacentShards(streamName, lowerShard,
								splitUpper.getLowerShard()).doMerge(kinesisClient,
										shardStack.isEmpty() ? splitUpper.getHigherShard().getShardId()
												: shardStack.lastElement().getShardId());
						LOG.debug(String.format("Created Shard %s (%s)", lowerMerged.getShardId(),
								pctFormat.format(lowerMerged.getPctWidth())));
						shardsCompleted++;

						// count of shards is unchanged in this case as we've
						// just rebalanced, so current count is not updated
					} else {
						// The lower and upper shards together are smaller than
						// the target size, so merge the two shards together
						ShardHashInfo lowerMerged = new AdjacentShards(streamName, lowerShard, higherShard)
								.doMerge(kinesisClient, shardStack.isEmpty() ? higherShard.getShardId()
										: shardStack.lastElement().getShardId());
						shardsCompleted++;
						currentCount--;

						// put the new shard back on the stack - it may still be
						// too small relative to the target
						shardStack.push(lowerMerged);
					}
				}
			} else if (StreamScalingUtils.softCompare(lowerShard.getPctWidth(), targetPct) == 0) {
				// at the correct size - move on
			} else {
				// lowest shard is larger than the target size so split at the
				// target offset
				AdjacentShards splitLower = lowerShard.doSplit(kinesisClient, targetPct,
						shardStack.isEmpty() ? lowerShard.getShardId() : shardStack.lastElement().getShardId());
				operationsMade++;

				LOG.debug(String.format("Split Shard %s at %s Creating Final Shard %s and Intermediate Shard %s (%s)",
						lowerShard.getShardId(), pctFormat.format(targetPct), splitLower.getLowerShard().getShardId(),
						splitLower.getHigherShard(), pctFormat.format(splitLower.getHigherShard().getPctWidth())));

				// push the higher of the two splits back onto the stack
				shardStack.push(splitLower.getHigherShard());
				shardsCompleted++;
				currentCount++;
			}
		} while (shardStack.size() > 0 || !shardStack.empty());

		return reportFor(streamName, operationsMade, scaleDirection);
	}

	private ScalingOperationReport scaleStream(String streamName, int originalShardCount, int targetShards,
			double targetPct, int operationsMade, int shardsCompleted, long startTime, Integer minShards,
			Integer maxShards) throws Exception {
		LOG.info(String.format("Scaling Stream %s from %s Shards to %s", streamName, originalShardCount, targetShards));

		return scaleStream(streamName, originalShardCount, targetShards, targetPct, operationsMade, shardsCompleted,
				startTime, getOpenShardStack(streamName), minShards, maxShards);
	}
}
