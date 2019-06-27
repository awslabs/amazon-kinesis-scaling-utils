#!/bin/bash
# set -x

ver=.9.5.9

# publish war to regional buckets
for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do aws s3 cp ../dist/KinesisAutoscaling-$ver.war s3://awslabs-code-$r/KinesisAutoscaling/ --acl public-read --region $r; done
