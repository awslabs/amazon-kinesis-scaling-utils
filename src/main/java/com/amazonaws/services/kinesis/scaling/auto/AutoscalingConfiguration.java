/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling.auto;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Transfer Object for the Autoscaling Configuration, which can be built from a
 * variety of file locations
 */
@SuppressWarnings("serial")
public class AutoscalingConfiguration implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(AutoscalingConfiguration.class);

	private static ObjectMapper mapper = new ObjectMapper();

	private String streamName;

	private String region = "us-east-1";

	private List<KinesisOperationType> scaleOnOperations;

	private ScalingConfig scaleUp;

	private ScalingConfig scaleDown;

	private Integer minShards;

	private Integer maxShards;

	private Integer refreshShardsNumberAfterMin = 10;

	private IScalingOperationReportListener scalingOperationReportListener;

	private Integer checkInterval = 45;

	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public List<KinesisOperationType> getScaleOnOperations() {
		return scaleOnOperations;
	}

	public void setScaleOnOperation(List<KinesisOperationType> scaleOnOperations) {
		this.scaleOnOperations = scaleOnOperations;
	}

	public ScalingConfig getScaleUp() {
		return scaleUp;
	}

	public void setScaleUp(ScalingConfig scaleUp) {
		this.scaleUp = scaleUp;
	}

	public ScalingConfig getScaleDown() {
		return scaleDown;
	}

	public void setScaleDown(ScalingConfig scaleDown) {
		this.scaleDown = scaleDown;
	}

	public Integer getMinShards() {
		return minShards;
	}

	public void setMinShards(Integer minShards) {
		this.minShards = minShards;
	}

	public Integer getMaxShards() {
		return maxShards;
	}

	public void setMaxShards(Integer maxShards) {
		this.maxShards = maxShards;
	}

	public Integer getRefreshShardsNumberAfterMin() {
		return refreshShardsNumberAfterMin;
	}

	public void setRefreshShardsNumberAfterMin(Integer refreshShardsNumberAfterMin) {
		this.refreshShardsNumberAfterMin = refreshShardsNumberAfterMin;
	}

	public IScalingOperationReportListener getScalingOperationReportListener() {
		return scalingOperationReportListener;
	}

	public Integer getCheckInterval() {
		return checkInterval;
	}

	public void setCheckInterval(Integer checkInterval) {
		this.checkInterval = checkInterval;
	}

	public static AutoscalingConfiguration[] loadFromURL(String url) throws IOException, InvalidConfigurationException {
		File configFile = null;

		if (url.startsWith("s3://")) {
			LOG.info(String.format("Downloading configuration file from %s", url));

			// download the configuration from S3
			S3Client s3Client = S3Client.builder().credentialsProvider(DefaultCredentialsProvider.builder().build())
					.build();

			// parse the config path to get the bucket name and prefix
			final String s3ProtoRegex = "s3:\\/\\/";
			String bucket = url.replaceAll(s3ProtoRegex, "").split("/")[0];
			String prefix = url.replaceAll(String.format("%s%s\\/", s3ProtoRegex, bucket), "");

			Path configFilePath = Paths.get(String.format("/tmp/%s", RandomStringUtils.randomAlphabetic(16)));

			s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(prefix).build(), configFilePath);
			s3Client.close();
			configFile = configFilePath.toFile();

			LOG.info(String.format("Loaded Configuration from Amazon S3 %s/%s to %s", bucket, prefix,
					configFilePath.getFileName()));
		} else if (url.startsWith("http")) {
			configFile = File.createTempFile("kinesis-autoscaling-config", null);
			LOG.info(String.format("Loading Configuration from %s to %s", url, configFile.getAbsolutePath()));
			FileUtils.copyURLToFile(new URL(url), configFile, 1000, 1000);
		} else {
			try {
				LOG.info(String.format("Loaded Configuration local %s", url));
				InputStream classpathConfig = AutoscalingConfiguration.class.getClassLoader().getResourceAsStream(url);
				if (classpathConfig != null && classpathConfig.available() > 0) {
					configFile = new File(
							AutoscalingConfiguration.class.getResource((url.startsWith("/") ? "" : "/") + url).toURI());
				} else {
					// last fallback to a FS location
					configFile = new File(url);

					if (!configFile.exists()) {
						throw new IOException("Unable to load local file from " + url);
					}
				}
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
		}

		// read the json config into an array of autoscaling configurations
		AutoscalingConfiguration[] configuration;
		try {
			configuration = mapper.readValue(configFile, AutoscalingConfiguration[].class);
		} catch (Exception e) {
			throw new InvalidConfigurationException(e);
		}

		// validate each of the configurations
		for (AutoscalingConfiguration conf : configuration) {
			conf.validate();
		}

		return configuration;
	}

	public void validate() throws InvalidConfigurationException {
		if (this.streamName == null || this.streamName.equals("")) {
			throw new InvalidConfigurationException("Stream Name must be specified");
		}

		if (this.scaleUp == null && this.scaleDown == null) {
			throw new InvalidConfigurationException("Must provide at least one scale up or scale down configuration");
		}

		if (this.scaleUp != null && this.scaleUp.getScalePct() != null && this.scaleUp.getScalePct() <= 100) {
			throw new InvalidConfigurationException(String.format(
					"Scale Up Percentage of %s is invalid or will result in unexpected behaviour. This parameter represents the target size of the stream after being multiplied by the current number of Shards. Scale up by 100%% will result in a Stream of the same number of Shards (Current Shard Count * 1)",
					this.scaleUp.getScalePct()));
		}

		if (this.scaleDown != null && this.scaleDown.getScalePct() != null && this.scaleDown.getScalePct() >= 100) {
			throw new InvalidConfigurationException(String.format(
					"Scale Down Percentage of %s is invalid or will result in unexpected behaviour. This parameter represents the target size of the stream after being multiplied by the current number of Shards. Scale up by 100%% will result in a Stream of the same number of Shards (Current Shard Count * 1)",
					this.scaleDown.getScalePct()));
		}

		if (this.minShards != null && this.maxShards != null && this.minShards > this.maxShards) {
			throw new InvalidConfigurationException("Min Shard Count must be less than Max Shard Count");
		}

		// set the default operation types to 'all' if none were provided
		if (this.scaleOnOperations == null || this.scaleOnOperations.size() == 0) {
			this.scaleOnOperations = Arrays.asList(KinesisOperationType.values());
		}

		// set a 0 cool off if none was provided
		if (this.scaleDown.getCoolOffMins() == null) {
			this.scaleDown.setCoolOffMins(0);
		}
		if (this.scaleUp.getCoolOffMins() == null) {
			this.scaleUp.setCoolOffMins(0);
		}
	}
}
