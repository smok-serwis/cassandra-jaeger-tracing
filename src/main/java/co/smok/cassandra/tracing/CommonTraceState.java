package co.smok.cassandra.tracing;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;


abstract public class CommonTraceState extends TraceState {
    protected CommonTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, Tracing.TraceType traceType) {
        super(coordinator, sessionId, traceType);
    }

    abstract public boolean isEmpty();
}
