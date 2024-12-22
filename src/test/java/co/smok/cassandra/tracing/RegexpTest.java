package co.smok.cassandra.tracing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.tag.Tag;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class RegexpTest
{
    static class MockSpan implements Span {
        final protected Map<String, String> tags = new HashMap<>();

        public MockSpan() {}

        @Override
        public SpanContext context() {
            return null;
        }

        @Override
        public Span setTag(String key, String value) {
            this.tags.put(key, value);
            return null;
        }

        @Override
        public Span setTag(String key, boolean value) {
            return null;
        }

        @Override
        public Span setTag(String key, Number value) {
            return null;
        }

        @Override
        public <T> Span setTag(Tag<T> tag, T value) {
            return null;
        }

        @Override
        public Span log(Map<String, ?> fields) {
            return null;
        }

        @Override
        public Span log(long timestampMicroseconds, Map<String, ?> fields) {
            return null;
        }

        @Override
        public Span log(String event) {
            return null;
        }

        @Override
        public Span log(long timestampMicroseconds, String event) {
            return null;
        }

        @Override
        public Span setBaggageItem(String key, String value) {
            return null;
        }

        @Override
        public String getBaggageItem(String key) {
            return "";
        }

        @Override
        public Span setOperationName(String operationName) {
            return null;
        }

        @Override
        public void finish() {

        }

        @Override
        public void finish(long finishMicros) {

        }
    }

    @Test
    public void multiRegexpsWork()
    {
        RegexpSeparator.AnalysisResult result = RegexpSeparator.match("Index mean cardinalities are a:1,b:2,c:3. Scanning with c,d,e.");
        MockSpan span = new MockSpan();
        result.applyTags(span);
        assertEquals("d", span.tags.get("index.1"));
        assertEquals("c:3", span.tags.get("index_estimate.2"));
    }
}
