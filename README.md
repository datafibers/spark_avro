##Apache Spark CSV to Avro Tools

#### SparkColumnFilterAvroProducer
* Read all test files as RDDs with textFiles
* Remove specific rows start from HEADER_IDENTIFIER or TRAILER_IDENTIFIER completely!
* The first n columns are removed by csv line size - schema size

#### SparkHeaderFilterAvroProducer
* Read whole test file as RDD to index line number in each text files with wholeTextFiles
* Remove header row (1st row) only and completely!

#### SparkLineFilterAvroProducer
* Read whole test file as RDD to index line number in each text files with wholeTextFiles
* Remove specific rows index from header or trailer as well as columns completely!
* The header and trailer number of rows are specified from headerRowRemoved and trailerRowRemoved
* The first n columns are removed by csv line size - schema size