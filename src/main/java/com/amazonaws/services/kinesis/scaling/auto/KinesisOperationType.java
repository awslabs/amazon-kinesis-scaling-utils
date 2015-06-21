/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.scaling.auto;

public enum KinesisOperationType {
    PUT {
        public int getMaxBytes() {
            return 1_048_576;
        }
        public int getMaxRecords() {
            return 1_000;
        }
    },
    GET {
        public int getMaxBytes() {
            return 2_097_152;
        }
        public int getMaxRecords() {
            return 2_000;
        }
    };

    public abstract int getMaxBytes();
    public abstract int getMaxRecords();
    
}
