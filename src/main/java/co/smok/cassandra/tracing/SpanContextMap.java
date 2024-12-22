package co.smok.cassandra.tracing;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A dual purpose class.
 *
 * First, a map that maps our contexts to particular traces
 * Second, a routing table that maps coordinator's sub-spans to contexts sent to replicas.
 */
public class SpanContextMap {
    final private Map<String, JaegerTraceState> map = new HashMap<>();

    private String contextToString(JaegerSpanContext context) {
        return context.getTraceId() + ":" + context.getSpanId();
    }

    public void put(JaegerSpanContext context, JaegerTraceState jts) {
        this.map.put(contextToString(context), jts);
    }

    public void put(JaegerTraceState state) {
        this.map.put(contextToString(state.parentSpan.context()), state);
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
