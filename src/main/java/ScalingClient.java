
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
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.scaling.ScaleDirection;
import com.amazonaws.services.kinesis.scaling.ScalingCompletionStatus;
import com.amazonaws.services.kinesis.scaling.ScalingOperationReport;
import com.amazonaws.services.kinesis.scaling.StreamScaler;
import com.amazonaws.services.kinesis.scaling.StreamScaler.ScalingAction;

import java.net.URI;
import java.util.Optional;

/**
 * Class which provides a host environment interface to working with the Kinesis
 * Scaling Utility<br>
 * <br>
 * All configuration options are specified using System Properties. An example
 * invocation to scale MyStream in eu-central-1 up by 10 shards would be invoked
 * as:<br>
 * java -cp $CLASSPATH -Dstream-name=MyStream -Dscaling-action=scaleUp
 * -Dcount=10 -Dregion=eu-central-1<br>
 * -Dmin-shards=N -Dmax-shards=M <br>
 * Options:
 * <li>stream-name - The name of the Stream to be scaled
 * <li>scaling-action - The action to be taken to scale. Must be one of
 * "scaleUp", "scaleDown","resize" or "report"
 * <li>count - Number of shards by which to absolutely scale up or down, or
 * resize to
 * <li>pct - Percentage of the existing number of shards by which to scale up or
 * down
 * <li>kinesis-endpoint - The endpoint address of the Kinesis Region where the
 * Stream exists
 */
public class ScalingClient {
	private StreamScaler scaler = null;

	/**
	 * Configuration name to be used for the Stream
	 */
	public static final String STREAM_PARAM = "stream-name";

	/**
	 * Configuration name to be used for the action to take when scaling
	 */
	public static final String ACTION_PARAM = "scaling-action";

	private StreamScaler.ScaleBy scaleBy;

	/**
	 * Configuration name to be used when you want to scale by an absolute
	 * number of Shards
	 */
	public static final String SCALE_COUNT_PARAM = "count";

	/**
	 * Configuration name to be used when you want to scale by a percentage of
	 * the current number of Shards
	 */
	public static final String SCALE_PCT_PARAM = "pct";

	public static final String REGION_PARAM = "region";

	public static final String ENDPOINT_PARAM = "kinesisEndpoint";

	public static final String SHARD_ID_PARAM = "shard-id";

	public static final String MIN_SHARDS_PARAM = "min-shards";

	public static final String MAX_SHARDS_PARAM = "max-shards";

	private String streamName;

	private String shardId;

	private Region region = Region.getRegion(Regions.US_EAST_1);

	private ScalingAction scalingAction;

	private int scaleCount;

	private double scalePct;

	private Integer minShards;

	private Integer maxShards;

	private Optional<URI> endpoint;

	private void loadParams() throws Exception {
		if (System.getProperty(STREAM_PARAM) == null) {
			throw new Exception("You must provide a Stream Name");
		} else {
			this.streamName = System.getProperty(STREAM_PARAM);
		}

		this.shardId = System.getProperty(SHARD_ID_PARAM);

		if (System.getProperty(ACTION_PARAM) == null) {
			throw new Exception("You must provide a Scaling Action");
		} else {
			this.scalingAction = ScalingAction.valueOf(System.getProperty(ACTION_PARAM));

			// ensure the action is one of the supported types for shards
			if (this.shardId != null && !(this.scalingAction.equals(StreamScaler.ScalingAction.split)
					|| this.scalingAction.equals(StreamScaler.ScalingAction.merge))) {
				throw new Exception("Can only Split or Merge Shards");
			}
		}

		if (System.getProperty(REGION_PARAM) != null) {
			this.region = Region.getRegion(Regions.fromName(System.getProperty(REGION_PARAM)));
		}

		this.endpoint = Optional.ofNullable(System.getProperty(ENDPOINT_PARAM)).map(URI::create);

		if (this.scalingAction != ScalingAction.report) {
			if (System.getProperty(SCALE_COUNT_PARAM) == null && System.getProperty(SCALE_PCT_PARAM) == null)
				throw new Exception("You must provide either a scaling Count or Percentage");

			if (System.getProperty(SCALE_COUNT_PARAM) != null && System.getProperty(SCALE_PCT_PARAM) != null)
				throw new Exception("You must provide either a scaling Count or Percentage but not both");

			if (this.shardId != null && System.getProperty(SCALE_COUNT_PARAM) == null) {
				throw new Exception("Shards must be scaled by an absolute number only");
			}

			if (System.getProperty(SCALE_COUNT_PARAM) != null) {
				this.scaleCount = Integer.parseInt(System.getProperty(SCALE_COUNT_PARAM));
				this.scaleBy = StreamScaler.ScaleBy.count;
			}

			if (System.getProperty(SCALE_PCT_PARAM) != null) {
				this.scalePct = Double.parseDouble(System.getProperty(SCALE_PCT_PARAM));
				this.scaleBy = StreamScaler.ScaleBy.pct;
			}

			if (System.getProperty(MIN_SHARDS_PARAM) != null) {
				this.minShards = Integer.parseInt(System.getProperty(MIN_SHARDS_PARAM));
			}

			if (System.getProperty(MAX_SHARDS_PARAM) != null) {
				this.maxShards = Integer.parseInt(System.getProperty(MAX_SHARDS_PARAM));
			}
		}

		scaler = new StreamScaler(Optional.of(this.region), this.endpoint);
	}

	private void run() throws Exception {
		loadParams();

		ScalingOperationReport report = null;

		switch (this.scalingAction) {
		case scaleUp:
			switch (this.scaleBy) {
			case count:
				report = scaler.scaleUp(this.streamName, this.scaleCount, this.minShards, this.maxShards);
				break;
			case pct:
				report = scaler.scaleUp(this.streamName, this.scalePct, this.minShards, this.maxShards);
				break;
			default:
				break;
			}
			break;
		case scaleDown:
			switch (this.scaleBy) {
			case count:
				report = scaler.scaleDown(this.streamName, this.scaleCount, this.minShards, this.maxShards);
				break;
			case pct:
				report = scaler.scaleDown(this.streamName, this.scalePct, this.minShards, this.maxShards);
				break;
			default:
				break;
			}
			break;
		case resize:
			switch (this.scaleBy) {
			case count:
				report = scaler.resize(this.streamName, this.scaleCount, this.minShards, this.maxShards);
				break;
			case pct:
				throw new Exception("Cannot resize by a Percentage");
				default:
					break;
			}
			break;
		case report:
			report = scaler.reportFor(ScalingCompletionStatus.ReportOnly, this.streamName, 0, ScaleDirection.NONE);
			default:
				break;
		}

		System.out.println("Scaling Operation Complete");
		System.out.println(report.toString());
	}

	/**
	 * Public host environment method used to invoke the Scaling Utility
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// override system properties with command line arguments
		if (args.length != 0) {
			System.setProperty(STREAM_PARAM, args[0]);
			System.setProperty(ACTION_PARAM, args[1]);
			System.setProperty(REGION_PARAM, args[2]);
		}
		new ScalingClient().run();
	}
}
