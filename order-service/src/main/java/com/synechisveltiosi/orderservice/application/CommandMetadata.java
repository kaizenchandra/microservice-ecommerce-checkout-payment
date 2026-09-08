package com.synechisveltiosi.orderservice.application;

import java.util.UUID;

public record CommandMetadata(UUID correlationId, UUID causationId, String traceparent) { }
