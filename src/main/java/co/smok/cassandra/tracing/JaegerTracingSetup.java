package co.smok.cassandra.tracing;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.BinaryCodec;
import io.jaegertracing.internal.propagation.TextMapCodec;
import io.opentracing.propagation.Format;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final public class JaegerTracingSetup {
    public static final String DEFAULT_TRACE_KEY = "uber-trace-id";
    public static final JaegerTracer tracer;
    public static final InetAddressAndPort coordinator = FBUtilities.getBroadcastAddressAndPort();
    private static final String JAEGER_TRACE_KEY_ENV_NAME = "JAEGER_TRACE_KEY";

    private static final Logger logger = LoggerFactory.getLogger(JaegerTracing.class);
    public static final String trace_key = (System.getenv(JAEGER_TRACE_KEY_ENV_NAME) == null) ?
            DEFAULT_TRACE_KEY : System.getenv(JAEGER_TRACE_KEY_ENV_NAME);

    static {

        Configuration.SenderConfiguration sender_cfg = Configuration.SenderConfiguration.fromEnv();
        final Configuration.ReporterConfiguration rc = Configuration.ReporterConfiguration.fromEnv().withSender(sender_cfg);

        Configuration.CodecConfiguration codec_cfg = new Configuration.CodecConfiguration().withCodec(
                Format.Builtin.TEXT_MAP,
                TextMapCodec.builder().withUrlEncoding(false)
                        .withSpanContextKey(trace_key)
                        .build());

        tracer = Configuration.fromEnv("c*:" + DatabaseDescriptor.getClusterName() + ":" + FBUtilities.getJustBroadcastAddress().getHostName()).withReporter(rc)
                .withCodec(codec_cfg)
                .getTracer();
        logger.warn("Using top use "+trace_key);
    }

}

