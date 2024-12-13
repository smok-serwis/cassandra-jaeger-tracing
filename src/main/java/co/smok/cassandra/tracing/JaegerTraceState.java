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
package co.smok.cassandra.tracing;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.clock.Clock;
import io.jaegertracing.internal.clock.SystemClock;
import io.opentracing.References;
import io.opentracing.Span;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.time.Instant;
import java.util.*;

/**
 * Thread-local for a tracing session. Considers only a single node and a single trace at given time.
 */
class JaegerTraceState extends TraceState {
    private static final long TIME_TO_WAIT_FOR_RESPONSES_IN_MCS = 3000000;
    private static final Clock clock = new SystemClock();
    private static final Logger logger = LoggerFactory.getLogger(JaegerTraceState.class);

    public JaegerSpan span = null;
    private volatile long timestamp;
    private volatile int refCount = 0;
    private boolean stopped = false;

    private boolean ignoreFirstTrace = false;

    private String firstMessage = null;
    protected JaegerSpan parentSpan = null;

    private boolean finished = false;

    public JaegerTracer.SpanBuilder spanBuilder = null;

    public JaegerTraceState(
                                        InetAddressAndPort coordinator,
                                        TimeUUID uuid,
                                        Tracing.TraceType traceType,
                                        JaegerSpan parentSpan
                            ) {
        super(coordinator, uuid, traceType);
        assert parentSpan != null;
        this.parentSpan = parentSpan;
        JaegerTracingSetup.tracer.activateSpan(this.parentSpan);
        this.timestamp = clock.currentTimeMicros();
        this.ignoreFirstTrace = false;
    }

    public JaegerTraceState(
            InetAddressAndPort coordinator,
            TimeUUID uuid,
            Tracing.TraceType traceType,
            JaegerTracer.SpanBuilder builder
    ) {
        super(coordinator, uuid, traceType);
        assert builder != null;
        this.spanBuilder = builder;
        this.timestamp = clock.currentTimeMicros();
        this.ignoreFirstTrace = true;
    }

    /**
     * Signal that it's ready to build the master span
     */
    public void setSpan() {
        assert this.parentSpan == null;
        assert this.spanBuilder != null;
        this.parentSpan = this.spanBuilder.withTag("started_at", Instant.now().toString()).withTag("thread", Thread.currentThread().getName()).start();
        JaegerTracingSetup.tracer.activateSpan(this.parentSpan);
        if (this.firstMessage != null) {
            this.traceImpl(this.firstMessage);
        }
    }

    @Override
    protected void traceImpl(String message) {
        // we do it that way because Cassandra calls trace() when an operation completes, not when it starts
        // as is expected by Jaeger

        if (this.ignoreFirstTrace) {
            this.firstMessage = message;
            this.ignoreFirstTrace = false;
            return;
        }

        if (this.parentSpan == null) {
            this.setSpan();
        }
        assert this.parentSpan != null;

        final RegexpSeparator.AnalysisResult analysis = RegexpSeparator.match(message);

        JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan(analysis.getTraceName())
                .withTag("thread", Thread.currentThread().getName())
                .withStartTimestamp(timestamp)
                .ignoreActiveSpan();

        builder.addReference(References.CHILD_OF, this.parentSpan.context());
        if (this.span != null) {
            builder.addReference(References.FOLLOWS_FROM, this.span.context());
        }

        final JaegerSpan span = builder.start();
        analysis.applyTags(span);
        span.finish();
        this.span = span;
        this.timestamp = clock.currentTimeMicros();
    }

    private synchronized long waitInterrupted(long period) {
        long microTimestamp = clock.currentTimeMicros();
        try {
            this.wait(period);
        } catch (InterruptedException e) {
            return clock.currentTimeMicros() - microTimestamp;
        }
        return 0;
    }

    @Override
    protected synchronized void waitForPendingEvents() {
        long timeToWait = TIME_TO_WAIT_FOR_RESPONSES_IN_MCS;
        while (timeToWait > 0) {
            timeToWait = waitInterrupted(timeToWait);
        }
        if (this.refCount > 0) {
            logger.warn("Got " + String.valueOf(this.refCount) + " references remaining");
        }
    }

    public synchronized void addRef() {
        this.refCount += 1;
    }

    public synchronized void subRef() {
        this.refCount -= 1;
        if (this.refCount == 0) {
            this.notifyAll();
        }
    }

    @Override
    /**
     * This has the side effect of stopping the parent span.
     */
    public synchronized void stop() {
        super.stop();
        if (!this.stopped) {
            logger.warn("Stopping a session with context"+this.parentSpan.context().toString());
            this.parentSpan.finish();
            this.parentSpan = null;
            this.stopped = true;
        }
    }
}
