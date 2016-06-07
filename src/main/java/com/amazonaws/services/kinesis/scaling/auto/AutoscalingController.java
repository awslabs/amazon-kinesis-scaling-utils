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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The AutoscalingController runs StreamMonitors in a Thread Pool for each of
 * the configured set of AutoscalingConfigurations provided
 */
public class AutoscalingController implements Runnable {
	public static final String CONFIGURATION = "autoscaling-config";

	public static final String CONFIG_URL_PARAM = "config-file-url";

	private static final Log LOG = LogFactory
			.getLog(AutoscalingController.class);

	// configurations we're responsible for
	private AutoscalingConfiguration[] config;

	// list of all currently running stream monitors
	private final Map<Integer, StreamMonitor> runningMonitors = new HashMap<>();

	private final Map<Integer, Future<?>> monitorFutures = new HashMap<>();

	// set up the executor thread pool
	private final ExecutorService executor = Executors.newFixedThreadPool(20);

	private static AutoscalingController controller;

	// controller is a singleton
	private AutoscalingController() throws Exception {
		throw new ExceptionInInitializerError();
	}

	private AutoscalingController(AutoscalingConfiguration[] config) {
		this.config = config;
	}

	public static AutoscalingController getInstance()
			throws InvalidConfigurationException {
		if (controller != null) {
			return controller;
		} else {
			String configPath = System.getProperty(CONFIG_URL_PARAM);

			if (configPath != null && !configPath.equals("")) {
				LOG.info("Starting Kinesis Autoscaling Agent");

				try {
					// read the json config into an array of autoscaling
					// configurations
					AutoscalingConfiguration[] config = AutoscalingConfiguration
							.loadFromURL(configPath);

					controller = getInstance(config);
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				}
			} else {
				throw new InvalidConfigurationException(
						String.format(
								"Unable to instantiate AutoscalingController without System Property %s",
								CONFIG_URL_PARAM));
			}

			return controller;
		}
	}

	public static AutoscalingController getInstance(
			AutoscalingConfiguration[] config) throws Exception {
		if (controller != null) {
			return controller;
		} else {
			try {
				controller = new AutoscalingController(config);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
			return controller;
		}
	}

	public void stopAll() throws Exception {
		for (Map.Entry<Integer, StreamMonitor> entry : runningMonitors.entrySet()) {
			StreamMonitor monitor = entry.getValue();
			LOG.info("Stopping Stream Monitor: "
					+ monitor.getConfig().getStreamName() + " ...");
			monitor.stop();
			// block until the Future returns that the Stream Monitor has
			// stopped
			monitorFutures.get(entry.getKey()).get();
			LOG.info("Stream Monitor: " + monitor.getConfig().getStreamName()
					+ " stopped");
		}
	}

	public void startMonitors() {
		// run all the configured monitors in a thread pool
		try {
			int i = 0;
			for (AutoscalingConfiguration c : this.config) {
				StreamMonitor monitor;
				try {
					LOG.info(String
							.format("AutoscalingController creating Stream Monitor for Stream %s",
									c.getStreamName()));
					monitor = new StreamMonitor(c, executor);
					runningMonitors.put(i, monitor);
					monitorFutures.put(i, executor.submit(monitor));
					i++;
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				}
			}

			// spin through all stream monitors to see if any failed
			while (true) {
				for (Map.Entry<Integer, Future<?>> entry : monitorFutures.entrySet()) {
					if (entry.getValue() == null) {
						throw new InterruptedException("Null Monitor Future");
					} else {
						if (entry.getValue().isDone()) {
							if (runningMonitors.get(entry.getKey()).getException() != null) {
								throw new InterruptedException(runningMonitors
										.get(entry.getKey()).getException().getMessage());
							}
						}
					}
				}

				Thread.sleep(60000);
			}
		} catch (InterruptedException e) {
			try {
				stopAll();

				// stop the executor service
				LOG.error(e.getMessage(), e);
				LOG.info("Terminating Thread Pool");
				executor.shutdown();
			} catch (Exception e1) {
				LOG.error(e1.getMessage(), e1);
			}
		}
	}

	/**
	 * Thread wrapper for startMonitors()
	 */
	@Override
	public void run() {
		try {
			AutoscalingController.getInstance().startMonitors();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	/**
	 * Interface for running as a Daemon Process
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		AutoscalingController.getInstance().startMonitors();
	}
}
