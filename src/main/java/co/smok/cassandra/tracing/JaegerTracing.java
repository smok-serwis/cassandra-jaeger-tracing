package co.smok.cassandra.tracing;

import io.jaegertracing.internal.JaegerSpanContext;
import io.opentracing.propagation.Format;
import org.apache.cassandra.concurrent.ExecutorLocals;
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
import java.util.HashMap;
import java.util.Map;

/**
 * These two come in two flavors: coordinator-based traces, created by newSession,
 * and child traces, created by initializeFromMessage.
 * <p>
 * This has two methods - get() and set(), which are thread-local, and a sessions, which is a map of UUID to traces,
 * and is NOT thread-local.
 * <p>
 * There is only one instance of this class, and it serves as a factory for TraceStates
 */
public final class JaegerTracing extends Tracing {
    private static final JaegerTracingSetup setup = new JaegerTracingSetup();

    private static final Logger logger = LoggerFactory.getLogger(JaegerTracing.class);
    private static final InetAddressAndPort localHost = InetAddressAndPort.getLocalHost();
    /**
     * This class has two purposes:
     * 1. a replacement for Tracing.sessions more adequate to our needs
     * 2. a routing table for responses to route them to correct JaegerTraceStates
     */


    // a replacement for Tracing.sessions - used to store all sessions, coordinator or replicas, but only those that we're responsible for
    final private SpanContextMap my_map = new SpanContextMap();
    // sub-spans of coordinator's parent span may issue requests of their own, we need to track them
    final private SpanContextMap routing_table = new SpanContextMap();

    /* a empty constructor is necessary for Cassandra to initialize this class **/
    public JaegerTracing() {
    }

    @Override
    public CommonTraceState get() {
        return (CommonTraceState) ExecutorLocals.current().traceState;
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
    public void stopSession() {
        CommonTraceState state = this.get();
        if (this.nullOrEmpty(state)) {
            return;
        }
        JaegerTraceState jts = (JaegerTraceState) state;
        jts.stop();
        my_map.remove(jts.parentSpan);
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
        CommonTraceState ts = this.get();
        if (this.nullOrEmpty(ts)) {
            return ts;
        }
        JaegerTraceState state = (JaegerTraceState) ts;

        state.parentSpan.setTag("request", request);
        String query = parameters.get("query");
        if (query != null) {
            state.parentSpan.setTag("db.statement", query);
        }
        if (client != null) {
            state.parentSpan.setTag("client", client.toString());
        }
        return state;
    }

    @Override
    // This is called only during tracing repairs, so it doesn't concern us
    public TraceState get(TimeUUID uuid) {
        return null;
    }

    private Map<String, byte[]> addContextToMap(JaegerSpanContext context) {
        return this.addContextToMap(context, new HashMap<>());
    }

    private Map<String, byte[]> addContextToMap(JaegerSpanContext context, Map<String, byte[]> params) {
        BinaryExtractor be = new BinaryExtractor();
        JaegerTracingSetup.tracer.inject(context, Format.Builtin.BINARY, be);
        params.putAll(be.toCustomMap());
        return params;
    }

    @Override
    // This is called by the thread that holds the current state, so we can safely attach it.
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable) {
        CommonTraceState ts = get();
        if (this.nullOrEmpty(ts)) {
            return addToMutable;
        }
        JaegerTraceState state = (JaegerTraceState) ts;
        JaegerSpanContext context_to_attach = state.getContextToAttach();

        if (state.isCoordinator) {
            this.routing_table.put(context_to_attach, state);
        }
        addToMutable.put(ParamType.CUSTOM_MAP, this.addContextToMap(context_to_attach));
        return addToMutable;
    }


    @Override
    /**
     * Called for non-local traces (traces that are not initiated by local node == coordinator).
     */
    public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
    }

    @Override
    /**
     * This is called by the replica when sending a message to coordinator, or by coordinator when sending to a replica.
     * But after headers have been attached
     */
    public void traceOutgoingMessage(Message<?> message, int serializedSize, InetAddressAndPort sendTo) {
        final JaegerSpanContext context = this.getContextFromHeader(message.header);
        if (context == null) {
            return;
        }

        if (message.header.verb.isResponse()) {
            // This is a response. We need to locate the parent span for it and notify it.
            JaegerTraceState ts = this.my_map.get(context);
            if (ts == null) {
                logger.info("Tracing outgoing message for unknown context {}", context);
                return;
            }
            ts.trace("Sending {} message to {} message size {} bytes", message.header.verb.toString(),
                    sendTo.toString(), serializedSize);
        } else {
            // Someone sent a request. Locate it, change the headers, and re-apply them.
            JaegerTraceState sender = this.routing_table.pop(context);
            sender.subRef();
            sender.trace("Sending {} message to {} message size {} bytes", message.header.verb.toString(),
                    sendTo.toString(), serializedSize);

            Map<String, byte[]> customParams = message.header.customParams();
            JaegerSpanContext new_context = sender.getContextToAttach();
            addContextToMap(new_context, customParams);
            this.routing_table.put(new_context, sender);
        }
    }

    private JaegerSpanContext getContextFromHeader(final Message.Header header) {
        final BinaryExtractor be = new BinaryExtractor(header.customParams());
        if (be.isEmpty()) {
            return null;
        }
        return JaegerTracingSetup.tracer.extract(Format.Builtin.BINARY, be);
    }

    @Override
    /**
     * This is invoked whether a replica receives a message from a coordinator, or the coordinator receives a response.
     *
     * This is allowed to return a null, but not allowed to set a thread-local state
     */
    public TraceState initializeFromMessage(final Message.Header header) {
        final JaegerSpanContext context = this.getContextFromHeader(header);
        if (context == null) {
            return null;
        }

        if (header.verb.isResponse()) {
            // It's a response, let's go find the responsible JaegerTraceState for it
            JaegerTraceState parent = this.routing_table.pop(context);
            if (parent == null) {
                logger.info("Received a time-outed replica for context {}", context);
                return null;
            }
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
    protected TimeUUID newSession(TimeUUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload) {
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
    public void doneWithNonLocalSession(TraceState state) {
    }


}
