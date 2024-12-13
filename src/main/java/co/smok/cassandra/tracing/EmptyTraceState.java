package co.smok.cassandra.tracing;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;

public class EmptyTraceState extends TraceState {

    protected EmptyTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, Tracing.TraceType traceType) {
        super(coordinator, sessionId, traceType);
    }

    @Override
    protected void traceImpl(String message) {

    }
}
