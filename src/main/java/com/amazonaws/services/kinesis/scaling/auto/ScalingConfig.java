/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto;

/**
 * Transfer object for a Scaling Action to take. An Autoscaling Configuration
 * will have multiple ScalingConfig, one for scale up action, and one for down
 */
public class ScalingConfig {

	private Integer scaleAfterMins, coolOffMins, scaleCount, scaleThresholdPct, scalePct;
	private String notificationARN;

	public Integer getScaleThresholdPct() {
		return scaleThresholdPct;
	}

	public void setScaleThresholdPct(int scaleThresholdPct) {
		this.scaleThresholdPct = scaleThresholdPct;
	}

	public Integer getScaleAfterMins() {
		return scaleAfterMins;
	}

	public void setScaleAfterMins(int scaleAfterMins) {
		this.scaleAfterMins = scaleAfterMins;
	}

	public Integer getCoolOffMins() {
		return coolOffMins;
	}

	public void setCoolOffMins(int coolOffMins) {
		this.coolOffMins = coolOffMins;
	}

	public Integer getScaleCount() {
		return scaleCount;
	}

	public void setScaleCount(int scaleCount) {
		this.scaleCount = scaleCount;
	}

	public Integer getScalePct() {
		return scalePct;
	}

	public void setScalePct(int scalePct) {
		this.scalePct = scalePct;
	}

	public String getNotificationARN() {
		return notificationARN;
	}

	public void setNotificationARN(String notificationARN) {
		this.notificationARN = notificationARN;
	}
}
