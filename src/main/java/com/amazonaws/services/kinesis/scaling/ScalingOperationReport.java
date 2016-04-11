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

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Transfer Object for the output of a Scaling Operation
 */
public class ScalingOperationReport {
	private Map<String, ShardHashInfo> layout;

	private int operationsMade;

	private ObjectMapper mapper = new ObjectMapper();

	public ScalingOperationReport(Map<String, ShardHashInfo> report) {
		this(report, 0);
	}

	public ScalingOperationReport(Map<String, ShardHashInfo> report, int operationsMade) {
		this.layout = report;
		this.operationsMade = operationsMade;

		mapper.enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN);
	}

	public Map<String, ShardHashInfo> getLayout() {
		return layout;
	}

	public int getOperationsMade() {
		return operationsMade;
	}

	public String asJson() throws Exception {
		return mapper.writeValueAsString(layout);
	}

	/**
	 * Generate a reader friendly report of the Shard topology
	 * 
	 * @param info
	 *            The Map of
	 *            {@link com.amazonaws.services.kinesis.scaling.ShardHashInfo}
	 *            objects indexed by Shard ID to report
	 * @return A String value useful for printing or logging
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		for (ShardHashInfo v : layout.values()) {
			sb.append(v.toString());
		}

		return sb.substring(0, sb.length() - 1);
	}
}
