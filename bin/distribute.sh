#!/bin/bash
# set -x

BASEDIR=$(dirname "$0")
ver=`cat $BASEDIR/../pom.xml | grep version | head -1 | cut -d"<" -f2 | cut -d">" -f2`

# publish war to regional buckets
for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do 
	echo "Starting background copy of version $ver to S3 AWSLabs Code Repo"
	aws s3 cp $BASEDIR/../dist/KinesisAutoscaling-$ver.war s3://awslabs-code-$r/KinesisAutoscaling/ --acl public-read --region $r --quiet &
	aws s3 cp $BASEDIR/../dist/KinesisScalingUtils-$ver-complete.jar s3://awslabs-code-$r/KinesisAutoscaling/ --acl public-read --region $r --quiet &
	aws s3 cp $BASEDIR/../dist/kinesis-scaling-utils-$ver.jar s3://awslabs-code-$r/KinesisAutoscaling/ --acl public-read --region $r --quiet &
done
