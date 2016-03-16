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
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * Immutable transfer object containing enhanced metadata about Shards in a
 * Stream, as well as utility methods for working with a Stream of Shards
 */
public class ShardHashInfo {
	private String streamName;

	private BigInteger startHash;

	private BigInteger endHash;

	private BigInteger hashWidth;

	private Double pctOfKeyspace;

	private Boolean matchesTargetResize;

	private Shard shard;

	private final NumberFormat pctFormat = NumberFormat.getPercentInstance();

	private static final BigInteger maxHash = new BigInteger("340282366920938463463374607431768211455");

	public ShardHashInfo(String streamName, Shard shard) {
		// prevent constructing a null object
		if (streamName == null || shard == null) {
			throw new ExceptionInInitializerError("Stream Name & Shard Required");
		}
		this.shard = shard;
		this.streamName = streamName;
		this.endHash = new BigInteger(shard.getHashKeyRange().getEndingHashKey());
		this.startHash = new BigInteger(shard.getHashKeyRange().getStartingHashKey());
		this.hashWidth = getWidth(this.startHash, this.endHash);
		this.pctOfKeyspace = getPctOfKeyspace(this.hashWidth);
	}

	public static BigInteger getWidth(BigInteger startHash, BigInteger endHash) {
		return endHash.subtract(startHash);
	}

	public static BigInteger getWidth(String startHash, String endHash) {
		return getWidth(new BigInteger(endHash), new BigInteger(startHash));
	}

	public static Double getPctOfKeyspace(BigInteger hashWidth) {
		return new BigDecimal(hashWidth).divide(new BigDecimal(maxHash), StreamScalingUtils.PCT_COMPARISON_SCALE,
				StreamScalingUtils.ROUNDING_MODE).doubleValue();
	}

	protected String getShardId() {
		return this.shard.getShardId();
	}

	protected Shard getShard() {
		return this.shard;
	}

	protected BigInteger getStartHash() {
		return this.startHash;
	}

	protected BigInteger getEndHash() {
		return this.endHash;
	}

	protected BigInteger getHashWidth() {
		return this.hashWidth;
	}

	protected double getPctWidth() {
		return this.pctOfKeyspace;
	}

	protected Boolean getMatchesTargetResize() {
		return matchesTargetResize;
	}

	protected BigInteger getHashAtPctOffset(double pct) {
		return this.startHash.add(new BigDecimal(maxHash).multiply(BigDecimal.valueOf(pct)).toBigInteger());
	}

	protected boolean isFirstShard() {
		return this.startHash.equals(BigInteger.valueOf(0l));
	}

	protected boolean isLastShard() {
		return this.endHash.equals(maxHash);
	}

	public String getStreamName() {
		return streamName;
	}

	/**
	 * Split the contained Shard at the indicated target percentage of keyspace
	 * 
	 * @param kinesisClient
	 * @param targetPct
	 * @return
	 * @throws Exception
	 */
	public AdjacentShards doSplit(AmazonKinesisClient kinesisClient, double targetPct, String currentHighestShardId)
			throws Exception {
		BigInteger targetHash = getHashAtPctOffset(targetPct);

		// split the shard
		StreamScalingUtils.splitShard(kinesisClient, this.streamName, this.getShardId(), targetHash, true);

		ShardHashInfo lowerShard = null;
		ShardHashInfo higherShard = null;

		// resolve the newly created shards from this one
		Map<String, ShardHashInfo> openShards = StreamScalingUtils.getOpenShards(kinesisClient, streamName,
				currentHighestShardId);

		for (ShardHashInfo info : openShards.values()) {
			if (!info.getShard().getShardId().equals(this.shard.getShardId())) {
				if (info.getShard().getHashKeyRange().getStartingHashKey().equals(targetHash.toString())) {
					higherShard = new ShardHashInfo(this.streamName, info.getShard());
					break;
				} else {
					lowerShard = new ShardHashInfo(this.streamName, info.getShard());
				}
			}
		}

		if (lowerShard == null || higherShard == null) {
			throw new Exception(String.format("Unable to resolve high/low shard mapping for Target Hash Value %s",
					targetHash.toString()));
		}

		return new AdjacentShards(streamName, lowerShard, higherShard);
	}

	@Override
	public String toString() {
		return String.format("Shard %s - Start: %s, End: %s, Keyspace Width: %s (%s)\n", this.getShardId(),
				this.getStartHash().toString(), this.getEndHash().toString(), this.getHashWidth().toString(),
				new DecimalFormat("#0.000%").format(this.getPctWidth()));
	}
}
