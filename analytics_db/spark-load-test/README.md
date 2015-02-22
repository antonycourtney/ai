# spark-csvtest

Simple test of using Spark to load a pipe-delimited data file resulting from an UNLOAD operation performed on Redshift.

# Build

To build the project run:

    $ sbt assembly

This will compile and build a combined jar, including the `lib/redshift-input-format` jar file.

# Run

Run the combined jar using:

    $ spark-submit --class CSVTest --master local[4] target/scala-2.10/CSVTest-assembly-1.0.jar

