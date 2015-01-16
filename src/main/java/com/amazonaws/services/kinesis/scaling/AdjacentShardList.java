/**
 * Amazon Kinesis Aggregators
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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Stack;

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
