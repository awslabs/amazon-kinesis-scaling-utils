/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
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

		for (Map.Entry<StreamMetric, Integer> entry : this.metrics.entrySet()) {
			sb.append(entry.getKey() + ":" + this.metrics.get(entry.getKey()) + "\n");
		}

		return sb.toString();
	}
}
