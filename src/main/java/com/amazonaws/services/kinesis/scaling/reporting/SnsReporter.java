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
