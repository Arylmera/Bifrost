package com.bnpparibasfortis.marvin.concurrency;

import java.time.Duration;

public class StructuredCallTimeoutException extends RuntimeException {
    public StructuredCallTimeoutException(String scopeName, Duration timeout, Throwable cause) {
        super("Structured scope '" + scopeName + "' exceeded timeout " + timeout, cause);
    }
}