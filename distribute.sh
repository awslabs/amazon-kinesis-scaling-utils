#!/bin/bash
# set -x

ver=`cat pom.xml | grep version | head -1 | cut -d">" -f2 | cut -d"<" -f1`

me="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# copy the distribution artefacts to dist
echo "Moving compiled artefacts to dist"
find $me/../target/*$ver* -type f -depth 0 -exec cp "{}" $me/../dist ";"

# publish war to regional buckets
for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do 
  echo "Copying dist/KinesisAutoscaling-$ver.war to s3://awslabs-code-$r"
  aws s3 cp $me/../dist/KinesisAutoscaling-$ver.war s3://awslabs-code-$r/KinesisAutoscaling/ --acl public-read --region $r; 
done
