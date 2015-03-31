/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2015, Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.scaling.reporting;

import com.amazonaws.services.kinesis.scaling.ScalingOperationReport;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.PublishRequest;

/**
 * Sends notifications to the provided ARN on scaling operation completion.
 */
public class SnsReporter {
    private final AmazonSNS amazonSNS;
    private final String snsReportingArn;

    public SnsReporter(AmazonSNS amazonSNS, String snsReportingArn) {
        this.amazonSNS = amazonSNS;
        this.snsReportingArn = snsReportingArn;
    }

    public void publishReport(ScalingOperationReport report) {
        amazonSNS.publish(new PublishRequest().withTopicArn(snsReportingArn).withMessage(report.toString()));
    }
}
