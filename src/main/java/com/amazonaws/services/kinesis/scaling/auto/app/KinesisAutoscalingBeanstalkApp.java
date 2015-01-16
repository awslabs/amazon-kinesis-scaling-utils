/**
 * Amazon Kinesis Aggregators Copyright 2014, Amazon.com, Inc. or its
 * affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the
 * License. A copy of the License is located at http://aws.amazon.com/asl/ or in
 * the "license" file accompanying this file. This file is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.kinesis.scaling.auto.app;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.scaling.auto.AutoscalingController;

public class KinesisAutoscalingBeanstalkApp implements ServletContextListener {
    private Thread streamMonitorController;

    private static final Log LOG = LogFactory.getLog(KinesisAutoscalingBeanstalkApp.class);

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        // stop all stream monitors in the Autoscaling Controller by
        // interrupting its thread
        streamMonitorController.interrupt();
    }

    @Override
    public void contextInitialized(ServletContextEvent contextEvent) {
        try {
            streamMonitorController = new Thread(AutoscalingController.getInstance());
            streamMonitorController.start();

        } catch (Exception e) {
            LOG.error(e);
        }
    }
}
