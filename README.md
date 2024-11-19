# A Jaeger tracing plugin for Cassandra
[![Publish package to GitHub Packages](https://github.com/smok-serwis/cassandra-jaeger-tracing/actions/workflows/build.yaml/badge.svg)](https://github.com/smok-serwis/cassandra-jaeger-tracing/actions/workflows/build.yaml)

Tested on Cassandra 5.0.2.

Cassandra provides [pluggable
tracing](https://web.archive.org/web/20160402125018/http://www.planetcassandra.org/blog/cassandra-3-4-release-overview/)
starting from version 3.4. In versions 4 and 4.1 it was significantly altered, but the maintainers
didn't tell us that. More changes were introduced by Cassandra 5. By adding a jar file to the Cassandra classpath and one JVM option, Cassandra's tracing can be
replaced with Jaeger. It can even identify incoming Jaeger traces and addCassandra's own internal tracing on to it.

### How to use even simpler

## How to use
- Make sure you have [Maven](https://maven.apache.org/) and
  [JDK](https://openjdk.java.net/) installed on your machine.
If you don't like to cloning and building Java, you can always type
-
```bash
mvn dependency:get -Dartifact=co.smok.cassandra:cassandra-jaeger-tracing:5.0.2
```
  But first make sure to configure GitHub's Maven repository. 
- Run following commands to build and place the jar
  ```sh
  # Cloning the repository
  git clone https://github.com/smok-serwis/cassandra-jaeger-tracing.git
  cd cassandra-jaeger-tracing

  # Create a jar file
  mvn package
  cp target/cassandra-jaeger-tracing-*-jar-with-dependencies.jar $CASSANDRA_HOME/lib/
  ```
  Here, `$CASSANDRA_HOME` is the directory where Cassandra is installed
- Start Cassandra with,
  ```sh
  JVM_OPTS\
  ="-Dcassandra.custom_tracing_class=co.smok.cassandra.tracing.JaegerTracing" \
  cassandra
  ```
  or edit the `jvm.options`

By default
[`jaeger-client-java`](https://github.com/jaegertracing/jaeger-client-java)
sends the spans to `localhost:6831` via UDP. This can be configured by
setting environment variables, `JAEGER_AGENT_HOST` and
`JAEGER_AGENT_PORT`. Refer [Configuration via
Environment](https://github.com/jaegertracing/jaeger-client-java/tree/master/jaeger-core#configuration-via-environment)
for more information.
You can also configure `JAEGER_ENDPOINT` instead to connect directly to collector. This is useful, because traces sometimes become quite large
and TCP is needed to shop them.

![cassandra-jaeger-tracing-select-query](https://user-images.githubusercontent.com/5154532/55792869-2ebf3300-5adf-11e9-9326-ad65f0e564ec.png
"SELECT * FROM t;")

## Background

This repository was originally based of [bhavin192'](https://github.com/infracloudio/cassandra-jaeger-tracing) plugin, 
whichi originally worked with Cassandra 3, however since multiple revisions of Cassandra have changed the tracing API this solution became unusable.

Since bhavin192 was reluctant to merge my pull requests, I've decided to take over the repository, rename it, and remove his code.

## Troubleshooting

When this tracing is used instead of Cassandra's default tracing, any
cqlsh statements run after enabling tracing with `TRACING ON;` are
going to time out eventually giving

``` 
Unable to fetch query trace: Trace information was not available within …
```

This is because traces are stored within different tables than Cassandra expects them to be (system_traces).
an easy fix around this behaviour in cqlsh is to reduce
`Session.max_trace_wait` down to 1 second.

### Continuing parent :
rT 
In order to continue a parent trace send the trace injected.
into custom_payload with the _trace_id_key_. Default is `uber-trace-id`, but it can be changed through an environment variable.
Inject it using HTTP_HEADERS TextMap codec with url encoding value of true.

Refer to your Cassandra driver documentation in order
to figure out how to send custom_payload.

If you need a custom trace key, specify it in environment
variable `JAEGER_TRACE_KEY`. Note that the default 
is `uber-trace-id`.
