package com.synechisveltiosi.saga;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Local OTLP/HTTP sink; decodes only the protobuf fields asserted by the integration test. */
final class OtlpCapture implements AutoCloseable {
    record ExportedSpan(String service, String traceId, String spanId, String parentId, String name) { }
    record Field(int number, byte[] value) { }
    final Queue<ExportedSpan> spans = new ConcurrentLinkedQueue<>();
    private final HttpServer server;
    OtlpCapture() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/traces", exchange -> {
            try { decode(exchange.getRequestBody().readAllBytes()); exchange.getResponseHeaders().set("Content-Type", "application/x-protobuf"); exchange.sendResponseHeaders(200, -1); }
            catch (RuntimeException error) { exchange.sendResponseHeaders(400, -1); }
            finally { exchange.close(); }
        }); server.start();
    }
    String endpoint() { return "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/traces"; }
    void decode(byte[] request) {
        for (var resourceSpans : values(request, 1)) {
            String service = "unknown";
            for (var resource : values(resourceSpans, 1)) for (var attribute : values(resource, 1))
                if (string(attribute, 1).equals("service.name")) service = string(first(attribute, 2), 1);
            for (var scope : values(resourceSpans, 2)) for (var span : values(scope, 2))
                spans.add(new ExportedSpan(service, hex(span, 1), hex(span, 2), hex(span, 4), string(span, 5)));
        }
    }
    static String hex(byte[] bytes, int field) { return HexFormat.of().formatHex(first(bytes, field)); }
    static String string(byte[] bytes, int field) { return new String(first(bytes, field), StandardCharsets.UTF_8); }
    static byte[] first(byte[] bytes, int field) { return values(bytes, field).stream().findFirst().orElse(new byte[0]); }
    static List<byte[]> values(byte[] bytes, int field) { return fields(bytes).stream().filter(f -> f.number() == field).map(Field::value).toList(); }
    static long varint(byte[] data, int[] offset) {
        long value = 0;
        for (int shift = 0; shift < 64; shift += 7) { int next = data[offset[0]++] & 255; value |= (long)(next & 127) << shift; if (next < 128) return value; }
        throw new IllegalArgumentException("Invalid protobuf varint");
    }
    static List<Field> fields(byte[] data) {
        var result = new ArrayList<Field>(); int[] offset = {0};
        while (offset[0] < data.length) {
            long tag = varint(data, offset); int kind = (int)(tag & 7);
            switch (kind) {
                case 0 -> varint(data, offset);
                case 1 -> offset[0] += 8;
                case 5 -> offset[0] += 4;
                case 2 -> { int length = Math.toIntExact(varint(data, offset)); int end = Math.addExact(offset[0], length);
                    if (length < 0 || end > data.length) throw new IllegalArgumentException("Invalid protobuf length");
                    result.add(new Field((int)(tag >>> 3), Arrays.copyOfRange(data, offset[0], end))); offset[0] = end; }
                default -> throw new IllegalArgumentException("Unsupported protobuf wire type");
            }
        }
        return result;
    }
    public void close() { server.stop(0); }
}
