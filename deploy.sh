#!/bin/bash
#set -x

ver=.9.8.3

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do 
	aws s3 cp dist/KinesisAutoscaling-$ver.war s3://awslabs-code-$r/KinesisAutoscaling/KinesisAutoscaling-$ver.war --acl public-read --region $r
done
