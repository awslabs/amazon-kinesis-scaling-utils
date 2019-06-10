/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.Statistic;

public enum KinesisOperationType {
	PUT {
		@Override
		public StreamMetrics getMaxCapacity() {
			StreamMetrics metrics = new StreamMetrics(PUT);
			metrics.put(StreamMetric.Bytes, 1_048_576);
			metrics.put(StreamMetric.Records, 1000);
			return metrics;
		}

		@Override
		public List<String> getMetricsToFetch() {
			List<String> metricsToFetch = new ArrayList<>();
			metricsToFetch.add("PutRecord.Bytes");
			metricsToFetch.add("PutRecords.Bytes");
			metricsToFetch.add("PutRecord.Success");
			metricsToFetch.add("PutRecords.Records");
			return metricsToFetch;
		}
	},
	GET {
		@Override
		public StreamMetrics getMaxCapacity() {
			StreamMetrics metrics = new StreamMetrics(GET);
			metrics.put(StreamMetric.Bytes, 2_097_152);
			metrics.put(StreamMetric.Records, 2000);
			return metrics;
		}

		@Override
		public List<String> getMetricsToFetch() {
			List<String> metricsToFetch = new ArrayList<>();
			metricsToFetch.add("GetRecords.Bytes");
			metricsToFetch.add("GetRecords.Success");
			return metricsToFetch;
		}
	};

	public abstract StreamMetrics getMaxCapacity();

	public abstract List<String> getMetricsToFetch();
}
