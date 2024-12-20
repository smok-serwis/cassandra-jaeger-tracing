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

    /**
     * This class has two purposes:
     * 1. a replacement for Tracing.sessions more adequate to our needs
     * 2. a routing table for responses to route them to correct JaegerTraceStates
      */

    private static class SpanContextMap {
        final private Map<String, JaegerTraceState> map = new HashMap<>();

        private String contextToString(JaegerSpanContext context) {
            return context.getTraceId()+":"+context.getSpanId();
        }

        public void put(JaegerSpanContext context, JaegerTraceState jts) {
            this.map.put(contextToString(context), jts);
        }

        public void put(JaegerTraceState state) {
            this.map.put(contextToString(state.parentSpan.context()), state);
        }

        public void put(JaegerSpan span, JaegerTraceState jts) {
            this.map.put(contextToString(span.context()), jts);
        }

        public JaegerTraceState get(JaegerSpanContext context) {
            return this.map.get(contextToString(context));
        }

        public JaegerTraceState pop(JaegerSpanContext context) {
            String ctxt = contextToString(context);
            JaegerTraceState state = this.map.get(ctxt);
            this.map.remove(ctxt);
            return state;
        }

        public void remove(JaegerSpanContext context) {
            this.map.remove(contextToString(context));
        }
        public void remove(JaegerSpan span) {
            this.map.remove(contextToString(span.context()));
        }
    }

    // a replacement for Tracing.sessions - used to store all sessions, coordinator or replicas, but only those that we're responsible for
    final private JaegerTracing.SpanContextMap my_map = new JaegerTracing.SpanContextMap();

    final private JaegerTracing.SpanContextMap routing_table = new JaegerTracing.SpanContextMap();

    private static final InetAddressAndPort localHost = InetAddressAndPort.getLocalHost();
    /* a empty constructor is necessary for Cassandra to initialize this class **/
    public JaegerTracing() {
    }

    @Override
    public CommonTraceState get()
    {
        return (CommonTraceState)ExecutorLocals.current().traceState;
    }

    /**
     * Stop the session processed by the current thread.
     * Everything was already done by our JaegerTraceState, including stopping the parent thread.
     */
    @Override
    protected void stopSessionImpl() {
    }

    /**
     * Register that a particular context should be mapped to this coordinator state
     */
    protected void registerRoutingTable(JaegerTraceState state, JaegerSpanContext context) {
        this.routing_table.put(context, state);
    }

    private boolean nullOrEmpty(CommonTraceState state) {
        return (state == null) || (state.isEmpty());
    }

    public CommonTraceState get(JaegerSpanContext context) {
        return this.my_map.get(context);
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
        my_map.remove(jts.parentSpan);
        logger.warn("Stopping coordinator session "+jts.parentSpan.context());
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
        return state;
    }

    @Override
    // This is called only during tracing repairs, so it doesn't concern us
    public TraceState get(TimeUUID uuid) {
        return null;
    }

    @Override
     // This is called by the thread that holds the current state, so we can safely attach it.
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable) {
        CommonTraceState ts = get();
        if (this.nullOrEmpty(ts)) {
            return addToMutable;
        }
        JaegerTraceState state = (JaegerTraceState)ts;
        JaegerTracing.BinaryExtractor be = new JaegerTracing.BinaryExtractor();
        JaegerSpanContext context_to_attach = state.getContextToAttach();
        JaegerTracingSetup.tracer.inject(context_to_attach, Format.Builtin.BINARY, be);

        if (state.isCoordinator) {
            logger.warn("I am a coordinator, my parentContext is "+state.parentSpan.context().toString()+" Im attaching "+context_to_attach.toString());
            this.routing_table.put(state.getContextToAttach(), state);
        } else {
            logger.warn("I am a replica, Im attaching "+context_to_attach.toString());
        }

        addToMutable.put(ParamType.CUSTOM_MAP, be.toCustomMap());
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
     * This is called by the replica when sending a message to coordinator, or by coordinator when sending to a replica.
     * But after headers have been attached
     */
    public void traceOutgoingMessage(Message<?> message, int serializedSize, InetAddressAndPort sendTo)
    {
        // We can't do much here, because we don't know who sent this message
    }


    @Override
    /**
     * This is invoked whether a replica receives a message from a coordinator, or the coordinator receives a response.
     *
     * This is allowed to return a null, but not allowed to set a thread-local state
     */
    public TraceState initializeFromMessage(final Message.Header header) {
        final BinaryExtractor be = new JaegerTracing.BinaryExtractor(header.customParams());
        if (be.isEmpty()) {
            return null;
        }
        final JaegerSpanContext context = JaegerTracingSetup.tracer.extract(Format.Builtin.BINARY, be);
        if (context == null) {
            return null;
        }

        if (header.verb.isResponse()) {
            // It's a response, let's go find the responsible JaegerTraceState for it
            JaegerTraceState parent = this.routing_table.pop(context);
            if (parent == null) {
                logger.warn("Received an invalid response type "+header.verb.toString()+" for context "+context.toString());
                return null;
            }
            logger.warn("Received a response for parent "+parent.parentSpan.context().toString()+" replicas contexts was "+context.toString());
            parent.responseReceived();
            return parent;
        } else {
            // It's a command from the coordinator
            // It's not a response, it's a command from a master
            JaegerTraceState trace = JaegerTraceState.asFollower(header.from, header.traceType(), context);
            my_map.put(trace);
            return trace;
       }
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
            traceState = new EmptyTraceState(localHost, traceType);
        } else {
            JaegerSpanContext context = JaegerTracingSetup.tracer.extract(Format.Builtin.TEXT_MAP, stm);
            if (context == null) {
                traceState = new EmptyTraceState(localHost, traceType);
            } else {
                JaegerTraceState jts = JaegerTraceState.asCoordinator(localHost, traceType, context);
                traceState = jts;
                my_map.put(jts);
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
        JaegerTraceState jts = (JaegerTraceState)cts;
        jts.stop();
        logger.warn("We are through with "+jts.parentSpan.context().toString());
        my_map.remove(jts.parentSpan);
    }

    static private class BinaryExtractor implements Binary {

        private ByteBuffer buffer = null;

        public boolean isEmpty() {
            return this.buffer == null;
        }

        public BinaryExtractor() {}

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
}
