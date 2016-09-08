amazon-kinesis-scaling-utils
============================

The Kinesis Scaling Utility is designed to give you the ability to scale Amazon Kinesis Streams in the same way that you scale EC2 Auto Scaling groups – up or down by a count or as a percentage of the total fleet. You can also simply scale to an exact number of Shards. There is no requirement for you to manage the allocation of the keyspace to Shards when using this API, as it is done automatically.

You can also deploy the Web Archive to a Java Application Server, and allow Scaling Utils to automatically manage the number of Shards in the Stream based on the observed PUT or GET rate of the stream. 

##Manually Managing your Stream##

You can manually run the Scaling Utility from the command line by calling the ScalingClient with the following syntax.

```
java -cp KinesisScalingUtils-complete.jar -Dstream-name=MyStream -Dscaling-action=scaleUp -Dcount=10 -Dregion=eu-west-1 ScalingClient

Options: 
stream-name - The name of the Stream to be scaled
scaling-action - The action to be taken to scale. Must be one of "scaleUp", "scaleDown", "resize", or "report"
count - Number of shards by which to absolutely scale up or down, or resize to or:
pct - Percentage of the existing number of shards by which to scale up or down
min-shards - The minimum number of shards to maintain
max-shards - The maximum number of shards which will cap the scaling operation
region - The Region where the Stream exists, such as us-east-1 or eu-west-1 (default us-east-1)
shard-id - The Shard which you want to target for Scaling. NOTE: This will create imbalanced partitioning of the Keyspace
```

You can also integrate the StreamScaler class with existing control systems.

Here are some useful shortcuts:

### Scale a Stream up by 10 Shards

```java -cp dist/KinesisScalingUtils-complete.jar -Dstream-name=MyStream -Dscaling-action=scaleUp -Dcount=10 -Dregion=eu-west-1 ScalingClient```

### Generate a report of Shard Keyspace Sizing

```java -cp dist/KinesisScalingUtils-complete.jar -Dstream-name=MyStream -Dscaling-action=report -Dregion=eu-west-1 ScalingClient```

### Scale up a specific Shard by making it into 3 equally sized Shards

```java -cp dist/KinesisScalingUtils-complete.jar -Dstream-name=MyStream -Dscaling-action=scaleUp -Dcount=3 -Dshard-id=shard-0000000000 -Dregion=eu-west-1 ScalingClient```

##Kinesis Autoscaling##

The Kinesis Autoscaling WAR can be deployed as an Elastic Beanstalk application, or to any Java application server, and once configured will monitor the CloudWatch statistics for your Stream and scale up and down as you configure it. Below you can see a graph of how Autoscaling will keep adequate Shard capacity to deal with PUT or GET demand:

![AutoscalingGraph](https://s3-eu-west-1.amazonaws.com/meyersi-ire-aws/KinesisScalingUtility/img/KinesisAutoscalingGraph.png)

To get started, create a new Elastic Beanstalk application which is a Web Server with a Tomcat predefined configuration. Deploy the WAR by uploading from your local GitHub copy of [dist/KinesisAutoscaling.war](dist/KinesisAutoscaling.war).

Once deployed, you must configure the Autoscaling engine by providing a JSON configuration file on an HTTP or S3 URL. The structure of this configuration file is as follows:

```
[streamMonitor1, streamMonitor2...streamMonitorN]
```

a streamMonitor object is a definition of an Autoscaling Policy applied to a Kinesis Stream, and this array allows a single Autoscaling Web App to monitor multiple streams. A streamMonitor object is configured by:

```
{"streamName":"String - name of the Stream to be Monitored",
 "region":"String - a Valid AWS Region Code, such as us-east-1 or eu-west-1",
 "scaleOnOperation":"List<String> - the types of metric to be monitored, including PUT or GET. Both PutRecord and PutRecords are monitored with PUT",
 "minShards":"Integer - the minimum number of Shards to maintain in the Stream at all times",
 "maxShards":"Integer - the maximum number of Shards to have in the Stream regardless of capacity used",
 "refreshShardsNumberAfterMin":"Integer - minutes interval after which the Stream Monitor should refresh the Shard count on the stream, to accomodate manual scaling activities. If unset, defaults to 10 minutes"
 "scaleUp": {
     "scaleThresholdPct":Integer - at what threshold we should scale up,
     "scaleAfterMins":Integer - how many minutes above the scaleThresholdPct we should wait before scaling up,
     "scaleCount":Integer - number of Shards to scale up by (prevails over scalePct),
     "scalePct":Integer - % of current Stream capacity to scale up by,
     "notificationARN" : String - the ARN of an SNS Topic to send notifications to after a scaleUp action has been taken
 },
 "scaleDown":{
     "scaleThresholdPct":Integer - at what threshold we should scale down,
     "scaleAfterMins":Integer - how many minutes below the scaleThresholdPct we should wait before scaling down,
     "scaleCount":Integer - number of Shards to scale down by (prevails over scalePct),
     "scalePct":Integer - % of current Stream capacity to scale down by,
     "coolOffMins":Integer - number of minutes to wait after a Stream scale down before we scale down again,
     "notificationARN" : String - the ARN of an SNS Topic to send notifications to after a scaleDown action has been taken
 }
}
```

once you've built the Autoscaling configuration required, save it to an HTTP file server or to Amazon S3. Then, access your Elastic Beanstalk application, and select 'Configuration' from the left hand Navigation Menu. Then select the 'Software Configuration' panel, and add a new configuration item called 'config-file-url' that points to the URL of the configuration file. Acceptable formats are 'http://path to file' or 's3://bucket/path to file'. Save the configuration, and then check the application logs for correct operation.

## Autoscaling Behaviour ##

In version .9.5.0, Autoscaling added the ability to scale on the basis of PUT ___and___ GET utilisation. This change means that you carefully have to consider your actual utilisation of each metric prior to configuring autoscaling with both metrics. For information on how the AutoScaling module will react with both metrics, consider the following table:

| | | PUT | | |
| :-- | :-- | :--: | :--: | :--: |
| | __Range__ | Below | In | Above |
|__GET__ | Below | Down | Down | Up |
| | In | Down | Do Nothing | Up |
| | Above | Up | Up | Up
