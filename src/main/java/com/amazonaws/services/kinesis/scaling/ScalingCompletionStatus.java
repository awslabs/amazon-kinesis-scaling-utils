/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling;

public enum ScalingCompletionStatus {
	ReportOnly, NoActionRequired, AlreadyAtMinimum, AlreadyAtMaximum, Error, Ok;
}
