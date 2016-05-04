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

import java.util.HashMap;
import java.util.Map;

public class StreamMetrics {
	private KinesisOperationType type;

	public StreamMetrics(KinesisOperationType type) {
		this.type = type;
	}

	private final Map<StreamMetric, Integer> metrics = new HashMap<>();

	public int put(StreamMetric m, int value) {
		Integer oldValue = metrics.put(m, value);
		if (oldValue != null) {
			return oldValue;
		}
		return 0;
	}

	public int get(StreamMetric m) {
		Integer metric = metrics.get(m);
		if (metric != null) {
			return metric;
		}
		return 0;
	}

	public KinesisOperationType getType() {
		return type;
	}

	public int increment(StreamMetric m, int value) {
		int metric = get(m);
		metric = metric + value;
		metrics.put(m, value);
		return metric;
	}

	public void incrementMultiple(StreamMetric m, int multiple) {
		int metric = get(m);
		put(m, metric * multiple);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		sb.append(String.format("Operation Type: %s", this.type) + "\n");

		for (StreamMetric metric : this.metrics.keySet()) {
			sb.append(metric + ":" + this.metrics.get(metric) + "\n");
		}

		return sb.toString();
	}
}
