/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto;

public enum StreamMetric {
	Bytes("BYTES"), Records("COUNT");

	private final String unit;

	StreamMetric(String u) {
		this.unit = u;
	}

	public static StreamMetric fromUnit(String unit) {
		for (StreamMetric m : values()) {
			if (m.unit.equals(unit)) {
				return m;
			}
		}
		return null;
	}
}
