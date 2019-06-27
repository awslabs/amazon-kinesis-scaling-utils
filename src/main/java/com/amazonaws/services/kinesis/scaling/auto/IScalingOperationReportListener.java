/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto;

import com.amazonaws.services.kinesis.scaling.ScalingOperationReport;

public interface IScalingOperationReportListener {

	void onReport(ScalingOperationReport report);

}
