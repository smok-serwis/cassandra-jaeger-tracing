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
import io.jaegertracing.internal.JaegerSpanContext;
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
class JaegerTraceState extends CommonTraceState {
    private static final long TIME_TO_WAIT_FOR_RESPONSES_IN_MCS = 3000000;
    private static final Clock clock = new SystemClock();
    private static final Logger logger = LoggerFactory.getLogger(JaegerTraceState.class);

    public JaegerSpan span = null;
    private volatile long timestamp;
    private volatile int refCount = 0;
    private boolean stopped = false;

    private boolean ignoreFirstTrace = false;

    private String firstMessage = null;
    public JaegerSpan parentSpan = null;

    private boolean isCoordinator;

    public JaegerTracer.SpanBuilder spanBuilder = null;


    public static JaegerTraceState asCoordinator(InetAddressAndPort coordinator, TimeUUID uuid, Tracing.TraceType traceType,
                                                 JaegerSpanContext context) {
        JaegerTraceState state = new JaegerTraceState(coordinator, uuid, traceType);
        JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan(traceType.toString())
                .asChildOf(context)
                .withTag("started_at", Instant.now().toString())
                .withTag("coordinator", coordinator.toString()).withTag("thread", Thread.currentThread().getName());
        state.parentSpan = builder.start();
        state.isCoordinator = true;
        state.ignoreFirstTrace = false;
        return state;
    }

    public static JaegerTraceState asFollower(InetAddressAndPort coordinator, TimeUUID uuid, Tracing.TraceType traceType,
                             JaegerTracer.SpanBuilder builder) {
        JaegerTraceState state = new JaegerTraceState(coordinator, uuid, traceType);
        state.spanBuilder = builder;
        state.isCoordinator = false;
        state.ignoreFirstTrace = true;
        return state;
    }

    private JaegerTraceState(InetAddressAndPort coordinator, TimeUUID uuid, Tracing.TraceType traceType) {
        super(coordinator, uuid, traceType);
    }

    /**
     * Make sure that we at least have a parent span to refer to
     */
    public void setSpan() {
        if (this.parentSpan != null) {
            return;
        }
        assert this.spanBuilder != null;

        this.parentSpan = this.spanBuilder.start();
        logger.warn("Starting a follower session for "+this.parentSpan.context().toString()+" with sessionId of "+this.sessionId.toString() );
        JaegerTracingSetup.tracer.activateSpan(this.parentSpan);
        if (this.firstMessage != null) {
            this.traceImpl(this.firstMessage);
        }
    }

    @Override
    /**
     * In case of followers: this will be called immediately after initializeFromMessage()
     */
    protected void traceImpl(String message) {
        // we do it that way because Cassandra calls trace() when an operation completes, not when it starts
        // as is expected by Jaeger
        this.setSpan();

        final RegexpSeparator.AnalysisResult analysis = RegexpSeparator.match(message);

        JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan(analysis.getTraceName())
                .withTag("thread", Thread.currentThread().getName())
                .withStartTimestamp(timestamp)
                .asChildOf(this.parentSpan);

        if (this.span != null) {
            builder.addReference(References.FOLLOWS_FROM, this.span.context());
        }

        final JaegerSpan c_span = builder.start();
        analysis.applyTags(c_span);
        c_span.finish();
        this.span = c_span;
        this.timestamp = clock.currentTimeMicros();
    }

    private long waitInterrupted(long period) {
        long microTimestamp = clock.currentTimeMicros();
        try {
            synchronized(this) {
                this.wait(period);
            }
        } catch (InterruptedException e) {
            return clock.currentTimeMicros() - microTimestamp;
        }
        return 0;
    }

    @Override
    protected void waitForPendingEvents() {
        long timeToWait = TIME_TO_WAIT_FOR_RESPONSES_IN_MCS;
        while ((timeToWait > 0) && (this.refCount >= 0)) {
            timeToWait = waitInterrupted(timeToWait);
        }
    }

    public void addRef() {
        if (!this.isCoordinator) {
            logger.warn("addRef() called on a non-coordinator");
        }
        if (this.refCount == 0) {
            return;
        }
        this.refCount += 1;
        return;
    }

    /**
     * @return return if all follower traces were acccounted for
     */
    public boolean subRef() {
        if (!this.isCoordinator) {
            logger.warn("subRef() called on a non-coordinator");
        }
        this.refCount -= 1;
        if (this.refCount == 0) {
            synchronized(this) {
                this.notifyAll();
            }
        }
        return this.refCount == 0;
    }

    @Override
    public void stop() {
        if (this.isCoordinator) {
            this.waitForPendingEvents();
        }
        this.parentSpan.finish();
        if (this.isCoordinator)  {
            logger.warn("Stopped coordinator session "+this.parentSpan.context().toString()+ " with sessionId of "+this.sessionId.toString());
        } else {
            logger.warn("Stopped follower session "+this.parentSpan.context().toString()+" with sessionId of "+this.sessionId.toString());
        }
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
