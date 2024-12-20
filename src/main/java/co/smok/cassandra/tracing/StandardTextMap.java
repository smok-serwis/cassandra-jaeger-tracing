package co.smok.cassandra.tracing;

import io.opentracing.propagation.TextMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class StandardTextMap implements TextMap {
    private static final Charset charset = StandardCharsets.UTF_8;
    private final Map<String, String> map = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(StandardTextMap.class);

    protected StandardTextMap() {
    }

    protected StandardTextMap(Map<String, ByteBuffer> custom_payload) {
        if (custom_payload != null) {
            for (Map.Entry<String, ByteBuffer> entry : custom_payload.entrySet()) {
                String key = entry.getKey();
                String value = charset.decode(entry.getValue()).toString();
                this.put(key, value);
            }
        }
    }

    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    /**
     * This will return an empty map if given null
     */
    static protected StandardTextMap fromCustomPayload(Map<String, byte[]> custom_payload) {
        if (custom_payload == null) {
            return new StandardTextMap();
        }
        StandardTextMap stm = new StandardTextMap();
        for (Map.Entry<String, byte[]> entry : custom_payload.entrySet()) {
            String key = entry.getKey();
            String value = charset.decode(ByteBuffer.wrap(entry.getValue())).toString();
            stm.put(key, value);
        }
        return stm;
    }

    static final private char FUCKING_SEMICOLON = ':';
    /** Because spurious spaces are inserted after the trace **/
    static private String filter(final String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (char c: s.toCharArray()) {
            if ((Character.digit(c, 16) != -1) || (c == FUCKING_SEMICOLON)) {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    protected static StandardTextMap copyFrom(Map<String, String> parameters) {
        final StandardTextMap stm = new StandardTextMap();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            stm.put(entry.getKey(), entry.getValue());
        }
        return stm;
    }

    public Map<String, byte[]> toBytes() {
        final Map<String, byte[]> my_map = new HashMap<>();
        for (Map.Entry<String, String> entry : this.map.entrySet()) {
            String to_process = entry.getValue();
            to_process = StandardTextMap.filter(to_process);
            my_map.put(entry.getKey(), charset.encode(to_process).array());
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
        for (Map.Entry<String, String> entry : this.map.entrySet()) {
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(">");
        return sb.toString();
    }

    @Override
    public void put(String s, String s1) {
        if (s1 == null)  {
            return;
        }
        map.put(s, StandardTextMap.filter(s1));
    }
}
