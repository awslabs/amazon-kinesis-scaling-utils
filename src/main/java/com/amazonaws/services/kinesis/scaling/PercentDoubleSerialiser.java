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
package com.amazonaws.services.kinesis.scaling;

import java.io.IOException;
import java.text.DecimalFormat;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class PercentDoubleSerialiser extends JsonSerializer<Double> {
	final DecimalFormat myFormatter = new DecimalFormat("#0.000%");

	public PercentDoubleSerialiser() {
	}

	public void serialize(Double value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonGenerationException {
		if (null == value) {
			// write the word 'null' if there's no value available
			jgen.writeNull();
		} else {
			final String output = myFormatter.format(value);
			jgen.writeNumber(output);
		}
	}
}
