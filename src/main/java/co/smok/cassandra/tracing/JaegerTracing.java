package co.smok.cassandra.tracing;

import ch.qos.logback.core.pattern.color.BoldYellowCompositeConverter;
import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.BinaryCodec;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.propagation.Binary;
import io.opentracing.propagation.BinaryExtract;
import io.opentracing.propagation.BinaryInject;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;
import org.jetbrains.annotations.NotNull;
import org.psjava.ds.array.Array;
import org.psjava.ds.array.DynamicArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * These two come in two flavors: coordinator-based traces, created by newSession,
 * and child traces, created by initializeFromMessage.
 *
 * This has two methods - get() and set(), which are thread-local, and a sessions, which is a map of UUID to traces,
 * and is NOT thread-local.
 *
 * There is only one instance of this class, and it serves as a factory for TraceStates
 */
public final class JaegerTracing extends Tracing {
    protected static final JaegerTracingSetup setup = new JaegerTracingSetup();

    private static final Logger logger = LoggerFactory.getLogger(JaegerTracing.class);

    private static class SpanContextMap {
        private Map<String, JaegerTraceState> map = new HashMap<>();

        public void put(JaegerSpanContext context, JaegerTraceState jts) {
            this.map.put(context.toString(), jts);
        }

        public JaegerTraceState get(JaegerSpanContext context) {
            return this.map.get(context.toString());
        }

        public void remove(JaegerSpanContext context) {
            this.map.remove(context.toString());
        }
    }

    JaegerTracing.SpanContextMap my_map = new JaegerTracing.SpanContextMap();

    private static final InetAddressAndPort localHost = InetAddressAndPort.getLocalHost();
    /* a empty constructor is necessary for Cassandra to initialize this class **/
    public JaegerTracing() {
    }

    @Override
    public CommonTraceState get()
    {
        return (CommonTraceState)ExecutorLocals.current().traceState;
    }

    @Override
    public CommonTraceState get(TimeUUID sessionId)
    {
        return (CommonTraceState)sessions.get(sessionId);
    }


    /**
     * Stop the session processed by the current thread.
     * Everything was already done by our JaegerTraceState, including stopping the parent thread.
     */
    @Override
    protected void stopSessionImpl() {
    }


    private boolean nullOrEmpty(CommonTraceState state) {
        return (state == null) || (state.isEmpty());
    }

    @Override
    /**
     * Called by coordinator when it's time to stop shop.
     */
    public void stopSession()
    {
        CommonTraceState state = get();
        if (this.nullOrEmpty(state))
        {
            return;
        }
        JaegerTraceState jts = (JaegerTraceState)state;
        jts.stop();
        sessions.remove(state.sessionId);
        my_map.remove(jts.parentSpan.context());
        set(null);
    }

    @Override
    /**
     * This is called at the coordinator to start tracing, after newSession()
     *
     * At this point the trace is set, and is available to obtain via get()
     *
     * @param parameters likely to contain info about queries
     */
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
        CommonTraceState ts = get();
        if (this.nullOrEmpty(ts)) {
            return ts;
        }
        JaegerTraceState state = (JaegerTraceState)ts;

        state.parentSpan.setTag("request", request);
        String query = parameters.get("query");
        if (query != null) {
            state.parentSpan.setTag("db.statement", client.toString());
        }
        if (client != null) {
            state.parentSpan.setTag("client", client.toString());
        }
        logger.warn("Started a coordinator session with "+state.parentSpan.context().toString());
        JaegerTracingSetup.tracer.activateSpan(state.parentSpan);
        return state;
    }

    @Override
    /**
     * This is called by the thread that holds the current state, so we can safely attach it.
     */
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable) {
        CommonTraceState ts = get();
        if (this.nullOrEmpty(ts)) {
            return addToMutable;
        }
        JaegerTraceState state = (JaegerTraceState)ts;
        JaegerSpan span = state.span;
        if (span == null) {
            // Apparently that did not help one bit
            throw new RuntimeException("Cannot attach trace headers when span is empty!");
        }
        StandardTextMap stm = new StandardTextMap();
        JaegerTracingSetup.tracer.inject(span.context(), Format.Builtin.TEXT_MAP, stm);
        addToMutable.put(ParamType.CUSTOM_MAP, stm.toBytes());
        addToMutable.put(ParamType.TRACE_SESSION, state.sessionId);
        return addToMutable;
    }


    @Override
    /**
     * Called for non-local traces (traces that are not initiated by local node == coordinator).
     */
    public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
        logger.warn("trace() non-local got called with"+message);
    }

    @Override
    /**
     * This is called by the replica when sending a message to coordinator, or by coordinator when sending to a replica
     */
    public void traceOutgoingMessage(Message<?> message, int serializedSize, InetAddressAndPort sendTo)
    {
        if (message.traceSession() == null) {
            return;
        }
        CommonTraceState t_s = get(message.header.customParams();
        if (this.nullOrEmpty(t_s)){
            return;
        }
        JaegerTraceState jts = (JaegerTraceState)t_s;

        jts.trace(String.format("Sending %s message to %s message size %s bytes", message.verb(), sendTo,
                serializedSize));

        if (message.verb().isResponse()) {
            doneWithNonLocalSession(jts);
        } else {
            jts.addRef();
        }
    }


    @Override
    /**
     * This is invoked whether a replica receives a message from a coordinator, or the coordinator receives a response.
     *
     * This is allowed to return a null, but not allowed to set a thread-local state
     */
    public TraceState initializeFromMessage(final Message.Header header) {
        if (header.customParams() == null) {
            return null;
        }
        TimeUUID sessionId = header.traceSession();
        if (sessionId == null) {
            return null;
        }

        CommonTraceState state = get(sessionId);
        if (state == null) {
            // We haven't heard of that session before

            if (header.verb.isResponse()) {
                logger.info("Misplaced message");
                return null;
            }

            final StandardTextMap stm = StandardTextMap.fromCustomPayload(header.customParams());
            if (stm == null) {
                return null;
            }

            final JaegerSpanContext context = JaegerTracingSetup.tracer.extract(Format.Builtin.TEXT_MAP, stm);
            if (context == null) {
                return null;
            }
            TimeUUID newSessionId = TimeUUID.Generator.nextTimeUUID();
            JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan(header.traceType().toString())
                    .asChildOf(context);
            JaegerTraceState trace = JaegerTraceState.asFollower(header.from, newSessionId, header.traceType(), builder);
            sessions.put(newSessionId, trace);
            return trace;
        }

        JaegerTraceState jts = (JaegerTraceState)state;
        if (header.verb.isResponse()) {
            jts.subRef();
            return jts;
        }
        return state;
    }

    @Override
    protected TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, Tracing.TraceType traceType) {
        assert false;   // should never get here
        return null;
    }

    /**
     * This is called only on coordinator node
     */
    protected TimeUUID newSession(TimeUUID sessionId, TraceType traceType, Map<String,ByteBuffer> customPayload)
    {
        assert get() == null;

        final StandardTextMap stm = new StandardTextMap(customPayload);
        TraceState traceState = null;
        if (stm.isEmpty()) {
            traceState = new EmptyTraceState(localHost, sessionId, traceType);
        } else {
            JaegerSpanContext context = JaegerTracingSetup.tracer.extract(Format.Builtin.TEXT_MAP, stm);
            if (context == null) {
                traceState = new EmptyTraceState(localHost, sessionId, traceType);
            } else {
                JaegerTraceState jts = JaegerTraceState.asCoordinator(localHost, sessionId, traceType, context);
                traceState = jts;
                sessions.put(sessionId, jts);
            }
        }

        set(traceState);
        return sessionId;
    }

    @Override
    /**
     * Called on followers, when they're through with their jobs.
     */
    public void doneWithNonLocalSession(TraceState state)
    {
        CommonTraceState cts = (CommonTraceState)state;
        if (this.nullOrEmpty(cts)) {
            return;
        }

        JaegerTraceState jts = (JaegerTraceState)state;
        jts.stop();
        sessions.remove(state.sessionId);
    }
}
