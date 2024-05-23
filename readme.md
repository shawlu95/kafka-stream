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
