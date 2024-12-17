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
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This is instantiated single
 */
public final class JaegerTracing extends Tracing {
    protected static final JaegerTracingSetup setup = new JaegerTracingSetup();

    private static final Logger logger = LoggerFactory.getLogger(JaegerTracing.class);

    private static final InetAddressAndPort localHost = InetAddressAndPort.getLocalHost();
    /* a empty constructor is necessary for Cassandra to initialize this class **/
    public JaegerTracing() {
    }

    /**
     * Stop the session processed by the current thread.
     * Everything was already done by our JaegerTraceState, including stopping the parent thread.
     */
    @Override
    protected void stopSessionImpl() {
    }

    @Override
    /**
     * This is called at the coordinator to start tracing.
     *
     * At this point the trace is set, and is available to obtain via get()
     *
     * @param parameters likely to contain info about queries
     */
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
        this.logger.info("begin({}, {}, {})", request, client, parameters.toString());
        TraceState t_state = get();

        assert t_state != null;

        if (t_state instanceof EmptyTraceState) {
            return t_state;
        }

        JaegerTraceState state = (JaegerTraceState)t_state;

        if (state.parentSpan == null) {
            state.setSpan();
            state.parentSpan.setTag("request", request);
            String query = parameters.get("query");
            if (query != null) {
                state.parentSpan.setTag("db.statement", client.toString());
            }
            if (client != null) {
                state.parentSpan.setTag("client", client.toString());
            }
        }
        set(state);
        return state;
    }


    @Override
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable) {
        if (get() == null) {
            return addToMutable;
        }
        if (get() instanceof EmptyTraceState) {
            return addToMutable;
        }
        JaegerTraceState state = (JaegerTraceState) get();
        JaegerSpan span = state.parentSpan;
        if (span == null) {
            // Apparerntly that did not help one bit
            logger.warn("Not today");
            return addToMutable;
        }
        StandardTextMap stm = new StandardTextMap();
        JaegerTracingSetup.tracer.inject(span.context(), Format.Builtin.TEXT_MAP, stm);
        logger.warn("Adding context: "+span.context().toString()+" stm="+stm.toString());
        addToMutable.put(
                ParamType.CUSTOM_MAP,
                stm.toBytes());
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
    public void traceOutgoingMessage(Message<?> message, int serializedSize, InetAddressAndPort sendTo)
    {
        if (message.traceSession() == null) {
            return;
        }
        TraceState t_s = get(message.traceSession());
        if (t_s == null) {
            return;
        }
        if (t_s instanceof EmptyTraceState) {
            return;
        }
        JaegerTraceState jts = (JaegerTraceState)t_s;

        if (message.verb().isResponse()) {
            jts.subRef();
        } else {
            jts.addRef();
            logger.warn("Tracing outgoing message " + message.verb().toString() + "going towards " + sendTo.toString() + " message has headers " + StandardTextMap.fromCustomPayload(message.header.customParams()).toString() + " and a sessionId of" + jts.sessionId.toString());
        }
    }


    @Override
    /**
     * This is invoked whether a replica receives a message from a coordinator, or the coordinator receives a response.
     *
     * This is allowed to return a null
     */
    public TraceState initializeFromMessage(final Message.Header header) {
        if (header.customParams() == null) {
            return null;
        }
        TimeUUID sessionId = header.traceSession();
        if (sessionId == null) {
            return null;
        }

        TraceState state = get(sessionId);
        if ((state == null) && (header.verb.isResponse())) {
            logger.warn("Spurious result seen, was response: "+header.verb.toString());
            return null;
        }

        if (header.verb.isResponse()) {
            if (state instanceof EmptyTraceState) {
                return null;
            }
            JaegerTraceState j_state = (JaegerTraceState) state;
            j_state.subRef();
            return state;
        }

        final StandardTextMap stm = StandardTextMap.fromCustomPayload(header.customParams());
        if (stm == null) {
            return null;
        }

        final JaegerSpanContext context = JaegerTracingSetup.tracer.extract(Format.Builtin.TEXT_MAP, stm);
        if (context == null) {
            return null;
        }
        JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan(header.traceType().toString()).asChildOf(context).ignoreActiveSpan();
        JaegerTraceState trace = new JaegerTraceState(header.from, sessionId, header.traceType(), builder);
        sessions.put(sessionId, trace);
        logger.warn("Initialized a subsession "+context.toString()+" with sessionId of "+sessionId.toString());
        return trace;
    }

    @Override
    protected TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, Tracing.TraceType traceType) {
        assert false;   // should never get here
        return null;
    }

    protected TimeUUID newSession(TimeUUID sessionId, TraceType traceType, Map<String,ByteBuffer> customPayload)
    {
        assert get() == null;

        final StandardTextMap stm = new StandardTextMap(customPayload);
        TraceState traceState = null;
        if ((stm == null) || (stm.isEmpty())) {
            traceState = new EmptyTraceState(localHost, sessionId, traceType);

        } else {
            JaegerSpanContext context = JaegerTracingSetup.tracer.extract(Format.Builtin.TEXT_MAP, stm);
            if (context == null) {
                traceState = new EmptyTraceState(localHost, sessionId, traceType);
            } else {
                JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan(traceType.toString()).asChildOf(context).withTag("started_at", Instant.now().toString()).withTag("coordinator", localHost.toString()).withTag("thread", Thread.currentThread().getName());
                traceState = new JaegerTraceState(localHost, sessionId, traceType, builder.start());
            }
        }

        set(traceState);
        sessions.put(sessionId, traceState);
        return sessionId;
    }

    @Override
    public void doneWithNonLocalSession(TraceState state)
    {
        try {
            JaegerTraceState jts = (JaegerTraceState)state;
            if (jts.subRef()) {
                sessions.remove(state.sessionId);
            }
        } catch (ClassCastException e) {
            return;
        }
    }
}
