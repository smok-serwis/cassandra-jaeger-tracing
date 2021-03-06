/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.infracloud.cassandra.tracing;

import io.opentracing.propagation.TextMap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class StandardTextMap implements TextMap {
    public static final StandardTextMap EMPTY_MAP = new StandardTextMap();
    private static final Charset charset = StandardCharsets.UTF_8;
    private final Map<String, String> map = new HashMap<>();

    protected StandardTextMap() {
    }

    protected StandardTextMap(Map<String, ByteBuffer> custom_payload) {
        for (Map.Entry<String, ByteBuffer> entry : custom_payload.entrySet()) {
            String key = entry.getKey();
            String value = charset.decode(entry.getValue()).toString();
            put(key, value);
        }
    }

    /**
     * An alternative constructor. Due to type erasure compatibility I cannot make
     * an overloaded constructor in Java
     */
    static public StandardTextMap from_bytes(Map<String, byte[]> custom_payload) {
        final Map<String, ByteBuffer> my_map = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : custom_payload.entrySet()) {
            my_map.put(entry.getKey(), ByteBuffer.wrap(entry.getValue()));
        }
        return new StandardTextMap(my_map);
    }

    public void injectToByteMap(Map<String, byte[]> my_map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            my_map.put(entry.getKey(), entry.getValue().getBytes(charset));
        }
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
