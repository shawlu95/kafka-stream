## Kafka Stream

- micro-batch: Spark, Nifi, Flink
- per data stream: Kafka stream
- no cluster required
- scales easily by adding java processes
- exactly once semantics

#### Stream Terminology

- steram: a stream is a sequence of immutable data recoreds, that fully ordered, can be replayed, and is fault tolerant
- stream processor: a node in the processor topology (graph); transforms incoming stream, record by record, and may create a new stream from it
- a topology is a graph of processor (node) and stream (edges)
- source processor: no predecessor. Directly read from **kafka topics**. Does not transform data
- sink processor: does not have shildren. Sends stream data directly to a kafka topic
- High-level DSL: simple, descriptive (declarative?); applicable to most stream applications
- Low-level Processor API: imperative, complex, rarely needed

#### KStream vs KTable

- stream: insert (append) only
  - use for non-compacted data
- table: upsert, more like db
  - keyed by primary key
  - update existing value
  - delete key-value, if receiving null value
  - use for compacted data
  - write to a topic, optimize recovery time, save storage
- a topic can be read as KStream or KTable, or GlobalKTable (similar to KTable)

#### Log Compaction

- huge improvement on performance
- retain AT LEAST the last known value of a specific key in a partition
- useful if you only need the snapshot instead of full history

#### Cheat Sheet

- two internal topics are created for each app: changelog, repartition
- use predicate to split stream into branches (multiple streams)
- MapValue turns each value into zero, one, or multiple values with the same key
- SelectKey redefines the key of stream
- stream marked for repartition if using: Map, FlatMap, SelectKey
- avoid repartition: MapValues, FlatMapValues
- table-stream duality: two representations, and can be converted into one another

#### Join

- two sources must be co-partitioned (same number of partitions)
- if one table is small, broadcast it by reading as GlobalKTable