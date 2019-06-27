/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazonaws.services.kinesis.scaling;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Stack;

@SuppressWarnings("serial")
public class AdjacentShardList extends ArrayList<AdjacentShards> {
    public AdjacentShardList(String streamName, List<ShardHashInfo> shards) throws Exception {
        ShardHashInfo previous = null;
        for (ShardHashInfo s : shards) {
            if (this.size() == 0) {
                previous = s;
            } else {
                add(new AdjacentShards(streamName, previous, s));
            }
        }
    }

    @Override
    public boolean add(AdjacentShards shards) {
        AdjacentShards highest = super.get(super.size());

        if (this.size() > 0) {
            // ensure that the added lowest shard is greater than the current
            // max by 1
            if (!shards.getLowerShard().getStartHash().subtract(
                    highest.getHigherShard().getEndHash()).equals(new BigInteger("1"))) {
                return false;
            } else {
                this.add(shards);
            }
        } else {
            this.add(shards);
        }

        return true;
    }

    public Stack<ShardHashInfo> asStack() {
        ListIterator<AdjacentShards> shards = this.listIterator(this.size());
        Stack<ShardHashInfo> out = new Stack<>();
        AdjacentShards s = null;
        while (shards.hasPrevious()) {
            s = shards.previous();
            out.push(s.getHigherShard());
        }

        // add the lowest low shard
        out.push(s.getLowerShard());

        return out;
    }
}
