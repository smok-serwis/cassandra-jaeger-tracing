package co.smok.cassandra.tracing;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.Tracing;

public class EmptyTraceState extends CommonTraceState {

    protected EmptyTraceState(InetAddressAndPort coordinator, Tracing.TraceType traceType) {
        super(coordinator, null, traceType);
    }

    @Override
    protected void traceImpl(String message) {

    }

    @Override
    public boolean isEmpty() {
        return true;
    }
}
