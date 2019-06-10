/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Transfer Object for the output of a Scaling Operation
 */
public class ScalingOperationReport {
	private Map<String, ShardHashInfo> layout;

	private int operationsMade;
	private ScaleDirection scaleDirection;
	private ScalingCompletionStatus endStatus;
	private ObjectMapper mapper = new ObjectMapper();

	public ScalingOperationReport(ScalingCompletionStatus endStatus, Map<String, ShardHashInfo> report) {
		this(endStatus, report, 0, ScaleDirection.NONE);
	}

	public ScalingOperationReport(ScalingCompletionStatus endStatus, Map<String, ShardHashInfo> report,
			int operationsMade, ScaleDirection scaleDirection) {
		this.endStatus = endStatus;
		this.layout = report;
		this.operationsMade = operationsMade;
		this.scaleDirection = scaleDirection;
	}

	public ScalingCompletionStatus getEndStatus() {
		return endStatus;
	}

	public Map<String, ShardHashInfo> getLayout() {
		return this.layout;
	}

	public int getOperationsMade() {
		return this.operationsMade;
	}

	public ScaleDirection getScaleDirection() {
		return this.scaleDirection;
	}

	public String asJson() throws Exception {
		return mapper.writeValueAsString(this);
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

		// add the direction
		sb.append(String.format("Scaling Direction: %s\n", this.scaleDirection));

		if (this.layout != null) {
			for (ShardHashInfo v : this.layout.values()) {
				sb.append(v.toString());
			}

			return sb.substring(0, sb.length() - 1);
		} else {
			return null;
		}
	}
}
