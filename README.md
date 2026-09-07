## Clojure Spark

A small Clojure project demonstrating Spark SQL and Flambo for processing JSON and Parquet data.

### Requirements

- Java 8 or a compatible JDK for Spark 2.4.8
- [Leiningen](https://leiningen.org/)

The project uses Clojure 1.10.3, Flambo 0.8.2, and Apache Spark 2.4.8 with Scala 2.11.

### Usage

Download dependencies and run the test suite with:

```sh
make deps
make test
```

The application entry point is `clojure-spark.core`. It reads sample JSON and Parquet resources under `resources/` and prints aggregated results.

To run it directly with Leiningen:

```sh
lein run
```

Generated build artifacts can be removed with:

```sh
make clean
```
