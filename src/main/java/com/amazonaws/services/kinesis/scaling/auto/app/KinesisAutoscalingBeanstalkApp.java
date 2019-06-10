/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto.app;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.scaling.auto.AutoscalingController;

public class KinesisAutoscalingBeanstalkApp implements ServletContextListener {
    private Thread streamMonitorController;

    private static final Log LOG = LogFactory.getLog(KinesisAutoscalingBeanstalkApp.class);

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        // stop all stream monitors in the Autoscaling Controller by
        // interrupting its thread
        streamMonitorController.interrupt();
		try {
			streamMonitorController.join();
		} catch (InterruptedException e) {
		}
    }

    @Override
    public void contextInitialized(ServletContextEvent contextEvent) {
        try {
            streamMonitorController = new Thread(AutoscalingController.getInstance());
            streamMonitorController.start();

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
