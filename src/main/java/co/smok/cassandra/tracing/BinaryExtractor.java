package co.smok.cassandra.tracing;

import io.opentracing.propagation.Binary;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class BinaryExtractor implements Binary {

    private ByteBuffer buffer = null;

    public boolean isEmpty() {
        return this.buffer == null;
    }

    public BinaryExtractor() {
    }

    public BinaryExtractor(Map<String, byte[]> map) {
        if (map == null) {
            return;
        }
        if (!map.containsKey(JaegerTracingSetup.trace_key)) {
            return;
        }
        this.buffer = ByteBuffer.wrap(map.get(JaegerTracingSetup.trace_key)).asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer extractionBuffer() {
        return this.buffer;
    }

    @Override
    public ByteBuffer injectionBuffer(int length) {
        this.buffer = ByteBuffer.allocate(length);
        return this.buffer;
    }

    public Map<String, byte[]> toCustomMap() {
        Map<String, byte[]> map = new HashMap<>();
        map.put(JaegerTracingSetup.trace_key, this.buffer.array());
        return map;
    }
}
