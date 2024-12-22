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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Thread-local for a tracing session. Considers only a single node and a single trace at given time.
 */
class JaegerTraceState extends CommonTraceState {
    private static final long TIME_TO_WAIT_FOR_RESPONSES_IN_MCS = 10000000; // 10 seconds
    private static final Clock clock = new SystemClock();
    private static final Logger logger = LoggerFactory.getLogger(JaegerTraceState.class);

    public JaegerSpan span = null;
    public volatile long timestamp;
    public JaegerSpan parentSpan = null;        // this is our private master span
    protected boolean isCoordinator;
    private volatile int refCount = 0;
    private JaegerSpanContext parentContext = null;

    private JaegerTraceState(InetAddressAndPort coordinator, Tracing.TraceType traceType) {
        super(coordinator, null, traceType);
        this.timestamp = clock.currentTimeMicros();
    }

    /**
     * Build a new JaegerTraceState in the role of a coordinator
     *
     * @param context context that was provided by an external service
     */
    public static JaegerTraceState asCoordinator(InetAddressAndPort coordinator, Tracing.TraceType traceType,
                                                 JaegerSpanContext context) {
        JaegerTraceState state = new JaegerTraceState(coordinator, traceType);
        JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan("Coordinator "+traceType.toString())
                .asChildOf(context).withStartTimestamp(state.timestamp)
                .withTag("started_at", Instant.now().toString())
                .withTag("coordinator", coordinator.toString()).withTag("thread", Thread.currentThread().getName());
        state.parentSpan = builder.start();
        state.isCoordinator = true;
        state.parentContext = context;
        return state;
    }

    /**
     * Activate the parent span
     **/
    public static JaegerTraceState asFollower(InetAddressAndPort coordinator, Tracing.TraceType traceType,
                                              JaegerSpanContext parentContext) {
        JaegerTraceState state = new JaegerTraceState(coordinator, traceType);
        state.parentSpan = JaegerTracingSetup.tracer.buildSpan("Replica "+traceType.toString()).asChildOf(parentContext).withStartTimestamp(state.timestamp).start();
        state.parentContext = parentContext;
        state.isCoordinator = false;
        return state;
    }

    /**
     * Returns a context to be attached to message sent somewhere.
     * Call it once per message.
     * WARNING! This method is NOT idempotent!
     */
    protected JaegerSpanContext generateContextToAttach() {
        if (this.isCoordinator) {       // this should return coordinator's current span
            assert this.span != null;
            JaegerSpanContext context = this.span.context();
            this.addRef();
            return context;
        } else {
            // This means that the replica is going to reply and it better be a context span that coordinator gave us
            return this.parentContext;
            // This trace state will be later closed during processing the request via traceOutgoingMessage()
        }
    }

    @Override
    public void trace(String message) {
        this.traceImpl(message);
    }

    @Override
    /**
     * In case of followers: this will be called immediately after initializeFromMessage()
     */
    protected void traceImpl(String message) {
        // we do it that way because Cassandra calls trace() when an operation completes, not when it starts
        // as is expected by Jaeger
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
            synchronized (this) {
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
        while ((timeToWait > 0) && (this.refCount > 0)) {
            timeToWait = waitInterrupted(timeToWait);
        }
    }

    public void responseReceived() {
        assert this.isCoordinator;
        this.subRef();
    }

    public void addRef() {
        if (this.refCount == 0) {
            return;
        }
        this.refCount += 1;
    }

    /**
     * @return return if all follower traces were acccounted for
     */
    public boolean subRef() {
        this.refCount -= 1;
        if (this.refCount == 0) {
            synchronized (this) {
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
        this.parentSpan.finish(clock.currentTimeMicros());
       }


    @Override
    public boolean isEmpty() {
        return false;
    }
}
