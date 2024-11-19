package co.smok.cassandra.tracing;

import io.opentracing.propagation.TextMap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class StandardTextMap implements TextMap {
    private static final Charset charset = StandardCharsets.UTF_8;
    private final Map<String, String> map = new HashMap<>();

    protected StandardTextMap() {
    }

    protected StandardTextMap(Map<String, ByteBuffer> custom_payload) {
        if (custom_payload != null) {
            for (Map.Entry<String, ByteBuffer> entry : custom_payload.entrySet()) {
                String key = entry.getKey();
                String value = charset.decode(entry.getValue()).toString();
                put(key, value);
            }
        }
    }


    protected static StandardTextMap copyFrom(Map<String, String> parameters) {
        final StandardTextMap stm = new StandardTextMap();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            stm.map.put(entry.getKey(), entry.getValue());
        }
        return stm;
    }


    public Map<String, ByteBuffer> toByteBuffer() {
        final Map<String, ByteBuffer> my_map = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            my_map.put(entry.getKey(), ByteBuffer.wrap(entry.getValue().getBytes(charset)));
        }
        return my_map;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    /**
     * This is for debug use only
     *
     * @return string representation of contents of the text map
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("StandardTextMap<");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(">");
        return sb.toString();
    }

    @Override
    public void put(String s, String s1) {
        map.put(s, s1);
    }
}
