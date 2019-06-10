/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling;

public class AlreadyOneShardException extends Exception {

	public AlreadyOneShardException() {
		super();
	}

	public AlreadyOneShardException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public AlreadyOneShardException(String message, Throwable cause) {
		super(message, cause);
	}

	public AlreadyOneShardException(String message) {
		super(message);
	}

	public AlreadyOneShardException(Throwable cause) {
		super(cause);
	}

}
