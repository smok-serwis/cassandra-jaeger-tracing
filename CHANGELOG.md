## v5.0.2

* made to support Cassandra 5.0.2
* intra-cassandra span transfers work (fixed #2) and will be done using binary codec
* got rid of CloserThread
* added more regexes, and improved them (fixed #3 and #4)

## v4.1.1

* Reporter will be configured from the environment

## v4.1.0

* Adapted to make use of [custom internode tracking](https://issues.apache.org/jira/browse/CASSANDRA-17981) introduced
  in Cassandra 4.1,  fixes #8, hopefully #10 as well asn
  [#15](https://github.com/infracloudio/cassandra-jaeger-tracing/pull/15) as well.
* An attempt was made to fix #12
