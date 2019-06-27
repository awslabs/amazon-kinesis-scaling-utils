/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
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
