/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling;

import java.math.BigInteger;
import java.util.Map;

import software.amazon.awssdk.services.kinesis.KinesisClient;

/**
 * AdjacentShards are a transfer object for maintaining references between an
 * open shard, and it's lower and higher neighbours by partition hash value
 */
public class AdjacentShards {
	private String streamName;

	private ShardHashInfo lowerShard;

	private ShardHashInfo higherShard;

	public AdjacentShards(String streamName, ShardHashInfo lower, ShardHashInfo higher) throws Exception {
		// ensure that the shards are adjacent
		if (!new BigInteger(higher.getShard().hashKeyRange().startingHashKey())
				.subtract(new BigInteger(lower.getShard().hashKeyRange().endingHashKey()))
				.equals(new BigInteger("1"))) {
			throw new Exception("Shards are not Adjacent");
		}
		this.streamName = streamName;
		this.lowerShard = lower;
		this.higherShard = higher;
	}

	protected ShardHashInfo getLowerShard() {
		return lowerShard;
	}

	protected ShardHashInfo getHigherShard() {
		return higherShard;
	}

	/**
	 * Merge these two Shards and return the result Shard
	 * 
	 * @param kinesisClient
	 * @return
	 * @throws Exception
	 */
	protected ShardHashInfo doMerge(KinesisClient kinesisClient, String currentHighestShardId) throws Exception {
		StreamScalingUtils.mergeShards(kinesisClient, streamName, this.lowerShard, this.higherShard, true);

		Map<String, ShardHashInfo> openShards = StreamScalingUtils.getOpenShards(kinesisClient, streamName,
				currentHighestShardId);

		for (ShardHashInfo info : openShards.values()) {
			if (lowerShard.getShardId().equals(info.getShard().parentShardId())
					&& higherShard.getShardId().equals(info.getShard().adjacentParentShardId())) {
				return new ShardHashInfo(streamName, info.getShard());
			}
		}

		throw new Exception(String.format("Unable resolve new created Shard for parents %s and %s",
				lowerShard.getShardId(), higherShard.getShardId()));
	}
}
