#!/bin/bash
#set -x

BASEDIR=$(dirname "$0")
ver=`cat $BASEDIR/../pom.xml | grep version | head -1 | cut -d"<" -f2 | cut -d">" -f2`

mvn
cp $BASEDIR/../target/*$ver.war $BASEDIR/../target/*$ver.jar $BASEDIR/../dist/
