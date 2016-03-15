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

import java.util.ArrayList;
import java.util.List;

public enum KinesisOperationType {
    PUT {
		@Override
        public StreamMetrics getMaxCapacity() {
			StreamMetrics metrics = new StreamMetrics();
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
			StreamMetrics metrics = new StreamMetrics();
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
