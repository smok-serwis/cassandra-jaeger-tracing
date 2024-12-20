=================
How does it work?
=================

An external client sends us it's context.

1. Coordinator builds it's own context as child of provided context via
   `JaegerTraceState.asCoordinator()` and saves it in `spanParent` property.
2. Coordinator's `JaegerTraceState` gets called with `.trace()`, which registers
   it's property `span`.
2. If there's a need to send a message somewhere:
   1. We'll use coordinator's `JaegerTraceState.span` context to attach tracing headers to the replica message.
      2. The replica will later attach exactly the same context
   2. We'll increment the reference count.
   3. Since the ingress point for incoming messages, including reply, is in `JaegerTracing` we'll need to inform it
      that such `span` was sent and to which `JaegerTraceState` route it to when it completes.
   4. Then the coordinator's `JaegerTraceState` will decrement the reference count.
3. The replica will 


# And so?

## Who calls what and in what sequence?
### Coordinator traces
JaegerTracing.begin - called by Native-Transport-Requests
JaegerTracing.addTraceHeaders - called by the same Native-Transport-Requests


### What might get called?

* addTraceHeaders() - seen on MutationStages, ReadStages - note that these do not have respective thread locals and know
  nothing about the sessions they're tracing
* initializeFromMessage() - only Messaging-EventLoops
* traceOutgoingMessage() - Messaging-EventLoops

## Sequences of calling
      it might happen that the same routine is called twice, so take care to note what headers you've already attached
JaegerTracing.begin is mostly called by  threads. The same threads stop them.
Later the s

Messaging-EventLoops receive responses from replicas.
