/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto;

import java.util.Arrays;
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
	
	public static final String SUPPRESS_ABORT_ON_FATAL = "suppress-abort-on-fatal";

	private static final Log LOG = LogFactory.getLog(AutoscalingController.class);

	// configurations we're responsible for
	private AutoscalingConfiguration[] config;

	// list of all currently running stream monitors
	private Map<Integer, StreamMonitor> runningMonitors = new HashMap<>();

	private Map<Integer, Future<?>> monitorFutures = new HashMap<>();

	// set up the executor thread pool
	private ExecutorService executor;

	private static AutoscalingController controller;

	// controller is a singleton
	private AutoscalingController() throws Exception {
		throw new ExceptionInInitializerError();
	}

	private AutoscalingController(AutoscalingConfiguration[] config) {
		this.config = config;
		this.executor = Executors.newFixedThreadPool(this.config.length);
	}

	private static void handleFatal(Exception e) {
		LOG.error("Fatal Exception while loading configuration file");
		LOG.error(e.getMessage(), e);
		
		// exit application unless abort on fatal is set - per Issue 66
		String suppressExit = System.getProperty(SUPPRESS_ABORT_ON_FATAL);
		if (suppressExit == null) {
			LOG.info("Supressing system exit based on environment configuration");
			System.exit(-1);
		}
	}
	
	public static AutoscalingController getInstance() throws InvalidConfigurationException {
		if (controller != null) {
			return controller;
		} else {
			String configPath = System.getProperty(CONFIG_URL_PARAM);

			if (configPath != null && !configPath.equals("")) {
				LOG.info("Starting Kinesis Autoscaling Agent");

				try {
					// read the json config into an array of autoscaling
					// configurations
					AutoscalingConfiguration[] config = AutoscalingConfiguration.loadFromURL(configPath);

					controller = getInstance(config);
				} catch (Exception e) {
					handleFatal(e);
				}
			} else {
				throw new InvalidConfigurationException(String.format(
						"Unable to instantiate AutoscalingController without System Property %s", CONFIG_URL_PARAM));
			}

			return controller;
		}
	}

	public static AutoscalingController getInstance(AutoscalingConfiguration[] config) throws Exception {
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
			LOG.info("Stopping Stream Monitor: " + monitor.getConfig().getStreamName() + " ...");
			monitor.stop();
			// block until the Future returns that the Stream Monitor has
			// stopped
			monitorFutures.get(entry.getKey()).get();
			LOG.info("Stream Monitor: " + monitor.getConfig().getStreamName() + " stopped");
		}
	}

	public void startMonitors() {
		// run all the configured monitors in a thread pool
		try {
			int i = 0;
			for (AutoscalingConfiguration streamConfig : this.config) {
				StreamMonitor monitor;
				try {
					LOG.info(String.format("AutoscalingController creating Stream Monitor for Stream %s",
							streamConfig.getStreamName()));
					monitor = new StreamMonitor(streamConfig);
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
								throw new InterruptedException(
										runningMonitors.get(entry.getKey()).getException().getMessage());
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
			handleFatal(e);
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
