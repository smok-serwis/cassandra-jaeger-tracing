package co.smok.cassandra.tracing;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
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


import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.BinaryCodec;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.propagation.Binary;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This is instantiated single
 */
public final class JaegerTracing extends Tracing {

    private static final String JAEGER_HEADER = "jaeger-header";
    private static final JaegerTracingSetup setup = new JaegerTracingSetup();

    private static final Logger logger = LoggerFactory.getLogger(JaegerTracing.class);


    /* a empty constructor is necessary for Cassandra to initialize this class **/
    public JaegerTracing() {}

    /**
     * Stop the session processed by the current thread
     */
    @Override
    protected void stopSessionImpl() {
        final JaegerTraceState state = (JaegerTraceState) get();
        if (state != null) {
            sessions.remove(state.sessionId);
            set(null);
        }
    }

    @Override
    /**
     * This is called at the coordinator to start tracing
     * This is meant to be used by implementations that need access to the message payload to begin their tracing.
     *
     * Since our tracing headers are to be found within the customPayload, this use case is warranted.
     *
     * @param customPayload note that this might be null
     */
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
        this.logger.trace("begin({}, {}, {})", request, client, parameters.toString());
        JaegerTraceState state = (JaegerTraceState) get();
        state.parentSpan.setTag("request", request);
        if (client != null) {
            state.parentSpan.setTag(Tags.SPAN_KIND_CLIENT, client.toString());
        }
        set(state);
        return state;
    }


    @Override
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable) {
        final Span span = JaegerTracingSetup.tracer.activeSpan();
        if (span != null) {
            final BinaryString bin_str = new BinaryString();
            final BinaryCodec bc = new BinaryCodec();
            bc.inject(((JaegerSpanContext)span.context()), bin_str);

            addToMutable.put(
                    ParamType.CUSTOM_MAP,
                    new LinkedHashMap<String, byte[]>() {{
                        put(JAEGER_HEADER, bin_str.bytes);
                    }});
        }
        return addToMutable;
    }

    @Override
    /**
     * Called for non-local traces (traces that are not initiated by local node == coordinator).
     */
    public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
        this.logger.trace("trace({}, {})", sessionId, message, ttl);
        final TimeUUID sessionUuid = TimeUUID.deserialize(sessionId);
        final TraceState state = sessions.get(sessionUuid);
        if (state == null) {
            return;
        }
        state.trace(message);
    }

    @Override
    public TraceState initializeFromMessage(final Message.Header header) {
        if (header.customParams() == null)
            return null;

        final byte[] bytes = header.customParams().get(JAEGER_HEADER);

        if (bytes == null)
            return null;
        // I did not write this using tracer's .extract and .inject() because I'm a Java noob - @piotrmaslanka
        final BinaryString bt = new JaegerTracing.BinaryString(bytes);
        final BinaryCodec bc = new BinaryCodec();
        final JaegerSpanContext context = bc.extract(bt);
        final JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan(header.verb.toString()).asChildOf(context);
        final JaegerSpan span = builder.start();
        return new JaegerTraceState(JaegerTracingSetup.tracer, header.from, header.traceSession(), header.traceType(),
                span);
    }

    @Override
    public TimeUUID newSession(Map<String, ByteBuffer> customPayload) {
        return this.myNewSession(TimeUUID.Generator.nextTimeUUID(), Tracing.TraceType.QUERY, new StandardTextMap(customPayload));
    }

    @Override
    public TimeUUID newSession(TraceType traceType) {
        return this.myNewSession(TimeUUID.Generator.nextTimeUUID(), traceType, new StandardTextMap());
    }

    @Override
    public TimeUUID newSession(TimeUUID sessionId, Map<String, ByteBuffer> customPayload) {
        return this.myNewSession(sessionId, Tracing.TraceType.QUERY, new StandardTextMap(customPayload));
    }

    private TimeUUID myNewSession(TimeUUID sessionId, TraceType traceType, StandardTextMap customPayload) {
        JaegerSpanContext parentSpan = JaegerTracingSetup.tracer.extract(Format.Builtin.HTTP_HEADERS, customPayload);
        // no need to trace if the parent is not sampled as well, aight?
        if (!parentSpan.isSampled()) {
            return null;
        }

        // this is valid even when parentSpan is null
        final TraceState ts = newTraceState(JaegerTracingSetup.coordinator, sessionId, traceType, parentSpan);
        set(ts);
        sessions.put(sessionId, ts);
        return sessionId;
    }

    private JaegerTraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, TraceType traceType,
                                     JaegerSpanContext span) {
        JaegerTracer.SpanBuilder sb = JaegerTracingSetup.tracer.buildSpan(traceType.toString());
        if (span != null) {
            sb = sb.addReference(References.CHILD_OF, span);
        }
        JaegerSpan currentSpan = sb.start();
        currentSpan.setTag("thread", Thread.currentThread().getName());
        currentSpan.setTag("sessionId", sessionId.toString());
        currentSpan.setTag("coordinator", coordinator.toString());
        currentSpan.setTag("started_at", Instant.now().toString());

        return  new JaegerTraceState(
                JaegerTracingSetup.tracer,
                coordinator,
                sessionId,
                traceType,
                currentSpan);
    }

    @Override
    protected TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, TraceType traceType) {
        return newTraceState(coordinator, sessionId, traceType, null);
    }

    public static class BinaryString extends BinaryCodec implements Binary {
        public byte[] bytes;

        public BinaryString() {
            this.bytes = new byte[64];
        }

        public BinaryString(byte[] arg) {
            this.bytes = arg;
        }


        @Override
        public ByteBuffer injectionBuffer(int i) {
            return ByteBuffer.wrap(this.bytes);
        }

        @Override
        public ByteBuffer extractionBuffer() {
            return ByteBuffer.wrap(this.bytes);
        }
    }

}
