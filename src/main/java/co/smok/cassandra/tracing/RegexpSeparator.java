/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.smok.cassandra.tracing;

import io.opentracing.Span;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class serves to identify common Cassandra trace messages,
 * splitting them with regexes to be set as tags
 */
public class RegexpSeparator {

    final static private SingleRegexp[] regexps = {
            new SingleRegexp("Sending message", "Sending (?<type>.*) message to (?<othernode>.*) message size (?<bytes>\\d+) bytes as (?<role>.*)",
                    new String[]{"othernode", "bytes", "type", "role"}),
            new SingleRegexp("Partition index found", "Partition index found for sstable (?<sstableid>\\d+), size = (?<size>\\d+)",
                    new String[]{"sstableid", "size"}),
            new SingleRegexp("Key cache hit",
                    "Key cache hit for sstable (?<sstableid>\\d+), size = (?<size>\\d+)",
                    new String[]{"sstableid", "size"}),
            new SingleRegexp("Parsing query",
                    "Parsing (?<query>.*)",
                    new String[]{"query"}),
            new SingleRegexp("Reading data",
                    "reading data from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Read live rows and tombstone cells",
                    "Read (?<liverows>\\d+) live rows and (?<tombstonecells>\\d+) tombstone cells",
                    new String[]{"liverows", "tombstonecells"}),
            new SingleRegexp("Merged data from memtables and sstables",
                    "Merged data from memtables and (?<sstables>\\d+) sstables",
                    new String[]{"sstables"}),
            new SingleRegexp("Skipped non-slice-intersecting sstables",
                    "Skipped (?<sstables>\\d+/\\d+) non-slice-intersecting sstables, included (?<tombstones>\\d+) due to tombstones",
                    new String[]{"sstables", "tombstones"}),
            new SingleRegexp("Enqueuing response",
                    "Enqueuing response to (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Sending message",
                    "Sending (?<type>.*) message to (?<othernode>.*)",
                    new String[]{"othernode", "type"}),
            new SingleRegexp("Message received",
                    "(?<type>.*) message received from (?<othernode>.*)",
                    new String[]{"othernode", "type"}),
            new SingleRegexp("Sending message",
                    "Sending (?<type>.*) message to (?<othernode>.*)",
                    new String[]{"othernode", "type"}),
            new SingleRegexp("Received message",
                    "(?<type>.*) message received from (?<othernode>.*)",
                    new String[]{"othernode", "type"}),
            new SingleRegexp("Scanned rows and matched ",
                    "Scanned (?<rows>\\d+) rows and matched (?<matched>\\d+)",
                    new String[]{"rows", "matched"}),
            new SingleRegexp("Partition index found",
                    "Partition index with (?<entries>\\d+) entries found for sstable (?<sstableid>\\d+)",
                    new String[]{"entries", "sstableid"}),
            new SingleRegexp("Speculating read retry",
                    "speculating read retry on (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Processing response",
                    "Processing response from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Bloom filter allows skipping sstable",
                    "Bloom filter allows skipping sstable (?<sstableid>\\d+)",
                    new String[]{"sstableid"}),
            new SingleRegexp("Caching rows",
                    "Caching (?<rows>\\d+) rows",
                    new String[]{"rows"}),
            new SingleRegexp("Executing single-partition query",
                    "Executing single-partition query on (?<table>.*)",
                    new String[]{"table"}),
            new SingleRegexp("Adding to memtable",
                    "Adding to (?<table>.*) memtable",
                    new String[]{"table"}),
            new SingleRegexp("Reading digest",
                    "reading digest from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Read-repair",
                    "Read-repair (?<consistency>.*)",
                    new String[]{"consistency"}),
            new SingleRegexp("Preparing","Preparing (?<paxosid>.*)", new String[]{"paxosid"}),
            new SingleRegexp("Promising ballot","Promising ballot (?<paxosid>.*)", new String[]{"paxosid"}),
            new SingleRegexp("Message received", "(?<type>.*) message received from (?<othernode>.*)",
                    new String[]{"othernode", "type"}),
            new SingleRegexp("Key cache hit", "Key cache hit for ssstable (?<sstable>\\d+), size = (?<size>\\d+)",
                    new String[]{"sstable", "size"}),
            new SingleRegexp("Enqueuing full request", "Enqueuing request to Full\\((?<othernode>.*),\\((?<start>-?\\d+),(?<stop>-?\\d+)]\\)",
                    new String[]{"othernode", "start", "stop"}),
            new MultiRegexp("Index mean cardinalities", "Index mean cardinalities are (?<indexesestimates>.*). Scanning with (?<indexes>.*).",
                    new String[]{"indexesestimates", "indexes"}, new String[]{"indexesestimates", "indexes"},
                    new String[]{"index_estimate", "index"}),
            new SingleRegexp("Executing read using an index", "Executing read on (?<keyspace>.*)\\.(?<table>.*) using index (?<index>.*)",
                    new String[]{"index", "table", "keyspace"}),
            new SingleRegexp("Executing seq scan", "Executing seq scan across (?<sstablecount>\\d+) sstables for \\(min\\((?<start>-?\\d+)\\), min\\((?<stop>-?\\d+)\\)]",
                    new String[]{"sstablecount", "start", "stop"}),
            new SingleRegexp("Submitted concurrent range requests", "Submitted (?<amount>\\d+) concurrent range requests",
                    new String[]{"amount"}),
            new SingleRegexp("Submitting range requests with a concurrency",
                    "Submitting range requests on (?<noranges>\\d+) ranges with a concurrency of (?<concurrency>\\d+) \\((?<rowsperrange>[0-9]*\\.?[0-9]*) rows per range expected\\)",
                    new String[]{"noranges", "concurrency", "rowsperrange"}),
            new SingleRegexp("Timed out", "Timed out; received (?<received>\\d+) of (?<expected>\\d+) responses",
                    new String[]{"received", "expected"})
    };

    static public AnalysisResult match(String trace) {
        for (final SingleRegexp srp : regexps) {
            final Matcher match = srp.match(trace);
            if (match.matches()) {
                return new RegexpResult(srp, match);
            }
        }
        return new NoMatch(trace);
    }

    abstract static public class AnalysisResult {
        /**
         * Return the name of the trace to use in Jaeger
         */
        abstract public String getTraceName();

        /**
         * Apply extracted tags to the span, or a no-op in case of a NoMatch
         */
        public void applyTags(Span span) {
        }
    }

    private static class NoMatch extends AnalysisResult {
        final private String trace;

        private NoMatch(final String trace) {
            this.trace = trace;
        }

        @Override
        public String getTraceName() {
            return trace;
        }
    }

    private static class RegexpResult extends AnalysisResult {
        final private SingleRegexp srp;
        final private Matcher match;

        private RegexpResult(SingleRegexp srp, Matcher match) {
            this.srp = srp;
            this.match = match;
        }

        @Override
        public String getTraceName() {
            return this.srp.label;
        }

        @Override
        public void applyTags(Span span) {
            if (this.srp instanceof MultiRegexp) {
                final MultiRegexp mrp = (MultiRegexp) this.srp;
                for (final String groupName : srp.namedGroups) {
                    // Check if it's a group that we should split
                    if (mrp.namedGroupsToSplit.contains(groupName)) {
                        int index = mrp.namedGroupsToSplit.indexOf(groupName);
                        String[] values = match.group(groupName).split(mrp.splitWith);
                        if (values.length > 1) {
                            final String tagNamePrefix = mrp.tagsNamePrefix[index];
                            for (int i = 0; i < values.length; i++) {
                                span.setTag(tagNamePrefix + "." + i, values[i]);
                            }
                        } else {
                            span.setTag(groupName, match.group(groupName));
                        }
                    }
                }
            } else {
                for (final String groupName : srp.namedGroups) {
                    span.setTag(groupName, match.group(groupName));
                }
            }
        }
    }

    private static class SingleRegexp {
        final private String label;
        final private Pattern pattern;
        // we provide named groups explicitly since there's no Java API to get
        // names of the groups from the matcher
        final private String[] namedGroups;

        private SingleRegexp(final String label, final String regexp, final String[] namedGroups) {
            this.pattern = Pattern.compile(regexp);
            this.label = label;
            this.namedGroups = namedGroups;
        }

        private Matcher match(final String trace) {
            return this.pattern.matcher(trace);
        }
    }

    /**
     * A regexp that can return an arbitrary number of tags
     */
    private static class MultiRegexp extends SingleRegexp {

        final private List<String> namedGroupsToSplit;
        final private String splitWith = ",";
        final private String[] tagsNamePrefix;

        private MultiRegexp(final String label, final String regexp, final String[] namedGroups, final String[] namedGroupsToSplit,
                            final String[] tagsNamePrefix) {
            super(label, regexp, namedGroups);
            this.namedGroupsToSplit = Arrays.asList(namedGroupsToSplit);
            this.tagsNamePrefix = tagsNamePrefix;
        }
    }

}
