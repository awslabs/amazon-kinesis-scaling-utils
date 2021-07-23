/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestScalingUtils {
	@Test
	public void testUnboundedScaleUpScenarios() {
		// Test whether we can fractionally scale up. Even a tiny amount of scale up
		// will be treated as a directive to scale
		assertEquals(2, StreamScalingUtils.getNewShardCount(1, null, 15, ScaleDirection.UP));

		// Test whether we can fractionally scale up with a larger %. This will be
		// treated as a raw addition to the overall number of shards
		assertEquals(2, StreamScalingUtils.getNewShardCount(1, null, 70, ScaleDirection.UP));
		assertEquals(17, StreamScalingUtils.getNewShardCount(10, null, 70, ScaleDirection.UP));

		// Test scaling up by a fraction greater than 100 is treated as an incremental
		// increase, not doubling
		assertEquals(6, StreamScalingUtils.getNewShardCount(5, null, 110, ScaleDirection.UP));

		// Test scaling by double
		assertEquals(14, StreamScalingUtils.getNewShardCount(7, null, 200, ScaleDirection.UP));

		// Test scaling by triple
		assertEquals(6, StreamScalingUtils.getNewShardCount(2, null, 300, ScaleDirection.UP));

		// Test scaling by more than 10x
		assertEquals(88, StreamScalingUtils.getNewShardCount(8, null, 1100, ScaleDirection.UP));

		// scaling down by a tiny fraction that's too small to yield a change
		assertEquals(3, StreamScalingUtils.getNewShardCount(3, null, 15, ScaleDirection.DOWN));
	}

	@Test
	public void testBoundedScalingScenarios() {
		// try to scale from 10 to 17 by scaling by 70%, with a max of 15
		assertEquals(15, StreamScalingUtils.getNewShardCount(10, null, 70, ScaleDirection.UP, null, 15));

		// try to scale down by 12x on a large shard count, but limit to 3 shards
		assertEquals(3, StreamScalingUtils.getNewShardCount(10, null, 1200, ScaleDirection.DOWN, 3, null));
	}

	@Test
	public void testUnboundedScaleDownScenarios() {
		// test edge condition of scaling down a single shard by a huge amount - should
		// never go below 1
		assertEquals(1, StreamScalingUtils.getNewShardCount(1, null, 500, ScaleDirection.DOWN));

		// test edge condition of scaling down large number of shards by a huge amount -
		// should never go below 1
		assertEquals(1, StreamScalingUtils.getNewShardCount(10, null, 1200, ScaleDirection.DOWN));

		// scale down by a fractional amount using both large and small shard counts
		// where we may/may not observe a change
		assertEquals(1, StreamScalingUtils.getNewShardCount(1, null, 20, ScaleDirection.DOWN));
		assertEquals(8, StreamScalingUtils.getNewShardCount(10, null, 20, ScaleDirection.DOWN));

		// scale down by 50% using 2 representations - both 50% and 200% are valid
		assertEquals(3, StreamScalingUtils.getNewShardCount(6, null, 50, ScaleDirection.DOWN));
		assertEquals(3, StreamScalingUtils.getNewShardCount(6, null, 200, ScaleDirection.DOWN));

		// scale down by a fractional percent - in this case 'down by 10%', which will
		// round down
		assertEquals(4, StreamScalingUtils.getNewShardCount(5, null, 110, ScaleDirection.DOWN));

		// now scale down by a factor, rounding down
		assertEquals(3, StreamScalingUtils.getNewShardCount(10, null, 300, ScaleDirection.DOWN));
	}
}
