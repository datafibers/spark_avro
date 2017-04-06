package org.sparkexample;

import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.io.StringReader;
import java.nio.file.Paths;

public class SparkColumnFilterAvroProducer implements Serializable {

    final static String HEADER_IDENTIFIER = "H";
    final static String TRAILER_IDENTIFIER = "T";
    final static char FILE_DELIMITER = '|';
    final static String FILE_LINE_END = "\n";
    final static String AVRO_FILE_WRITE_MODE = "overwrite"; // Can be append as well

    CSVParser csvParser = null;
    CSVFormat csvFileFormat = CSVFormat.EXCEL.withDelimiter(FILE_DELIMITER).withRecordSeparator(FILE_LINE_END);

    public static void main(String[] args) {
        // Run this only in Windows
        if (System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
            System.setProperty("hadoop.home.dir", Paths.get("src/main/resources").toAbsolutePath().toString());

        SparkColumnFilterAvroProducer wholeTextFiles = new SparkColumnFilterAvroProducer();
        wholeTextFiles.run(
                args[0], //Paths.get("src/main/resources/data").toAbsolutePath().toString(),
                args[1], //Paths.get("src/main/resources/schema", "twitter.avsc").toAbsolutePath().toString(),
                args[2] //Paths.get("src/main/resources/avro").toAbsolutePath().toString()
        );
    }

    private void run(String path, String schemaPath, String avroPath) {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkColumnFilterAvroProducer");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        String schemaString = String.join("", javaSparkContext.textFile(schemaPath).collect());
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        int avroSchemaSize = avroSchema.getFields().size();

        JavaRDD<String> content = javaSparkContext.textFile(path);

        JavaRDD<String> contentFiltered = content.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String content) throws Exception {
                csvParser = new CSVParser(new StringReader(content), csvFileFormat);
                CSVRecord csvLine = csvParser.getRecords().get(0);
                String rowIdentifier = csvLine.get(0);
                return !(rowIdentifier.equalsIgnoreCase(HEADER_IDENTIFIER) ||
                        rowIdentifier.equalsIgnoreCase(TRAILER_IDENTIFIER));
            }
        });

        JavaRDD<Row> rowRDD = contentFiltered
                .map(new Function<String, Row>() {
                    @Override
                    public Row call(String line) throws Exception {

                        csvParser = new CSVParser(new StringReader(line), csvFileFormat);
                        // Get first item only since we have one line
                        CSVRecord csvLine = csvParser.getRecords().get(0);
                        // Here remove the specific column, such as the first column with index = 0
                        //csvLine.
                        String[] columns = new String[avroSchemaSize];

                        for (int i = 1; i <= avroSchemaSize; i++) {
                            columns[i - 1] = csvLine.get(i);
                        }

                        return RowFactory.create(
                                SparkEngineUtility.structDecodingFromLine(
                                        columns,
                                        schemaString));
                    }
                });


        SQLContext sqlContext = new SQLContext(javaSparkContext);

        StructType schema = SparkEngineUtility.structTypeForSchema(avroSchema);

        sqlContext.createDataFrame(rowRDD, schema)
                .coalesce(1)
                .write()
                .mode(AVRO_FILE_WRITE_MODE)
                .format("com.databricks.spark.avro")
                .save(avroPath)
        ;

    }
}
