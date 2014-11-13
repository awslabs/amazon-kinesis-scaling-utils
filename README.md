amazon-kinesis-scaling-utils
============================

The Kinesis Scaling Utility is designed to give you the ability to scale Amazon Kinesis Streams in the same way that you scale EC2 Auto Scaling groups – up or down by a count or as a percentage of the total fleet. You can also simply scale to an exact number of Shards. There is no requirement for you to manage the allocation of the keyspace to Shards when using this API, as it is done automatically.

The utility might be used as part of a larger scaling environment, where you use the library directly from your application via the StreamScaler class. This allows you to call scaleUp or scaleDown with an increment or a percentage, or to set the exact number of shards required. Alternatively you might use the ScalingClient command line utility, which allows you to run the utility from any host environment with switches like “-Daction=scaleUp –Dcount=10"

##Usage##
```
java -cp KinesisScalingUtils.jar-complete.jar -Dstream-name=MyStream -Dscaling-action=scaleUp -Dcount=10 -Dregion=eu-west-1

Options: 
stream-name - The name of the Stream to be scaled
scaling-action - The action to be taken to scale. Must be one of "scaleUp”, "scaleDown" or “resize"
count - Number of shards by which to absolutely scale up or down, or resize to or:
pct - Percentage of the existing number of shards by which to scale up or down
region - The Region where the Stream exists (default US_EAST_1)
```
