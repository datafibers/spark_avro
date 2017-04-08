package org.datafibers;

import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.io.StringReader;

/**
 * CSVColumnFilterAvroProducer
 * <p>
 * Read all test files as RDDs with textFiles
 * Remove specific rows start from HEADER_IDENTIFIER or TRAILER_IDENTIFIER completely!
 * The first n columns are removed by csv line size - schema size
 */
public class CSVColumnFilterAvroProducer implements Serializable {

    final static String HEADER_IDENTIFIER = "H";
    final static String TRAILER_IDENTIFIER = "T";
    final static char FILE_DELIMITER = '|';
    final static String FILE_LINE_END = "\n";
    final static String AVRO_FILE_WRITE_MODE = "overwrite"; // Can be append as well

    CSVParser csvParser = null;
    CSVFormat csvFileFormat = CSVFormat.EXCEL.withDelimiter(FILE_DELIMITER).withRecordSeparator(FILE_LINE_END);

    public static void main(String[] args) {

        CSVColumnFilterAvroProducer wholeTextFiles = new CSVColumnFilterAvroProducer();
        wholeTextFiles.run(args[0], args[1], args[2]);
    }

    private void run(String path, String schemaPath, String avroPath) {

        String appName = "ColumnFilter - " + schemaPath.substring(
                schemaPath.lastIndexOf(System.getProperty("file.separator")) + 1,
                schemaPath.length());

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        String schemaString = String.join("", javaSparkContext.textFile(schemaPath).collect());
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        int avroSchemaSize = avroSchema.getFields().size();

        JavaRDD<String> content = javaSparkContext.textFile(path);
        JavaRDD<String> contentFiltered = content
                .filter(line -> !line.contains(HEADER_IDENTIFIER))
                .filter(line -> !line.contains(TRAILER_IDENTIFIER));

        JavaRDD<Row> rowRDD = contentFiltered
                .map(line -> {
                            csvParser = new CSVParser(new StringReader(line), csvFileFormat);
                            // Get first item only since we have one line
                            CSVRecord csvLine = csvParser.getRecords().get(0);
                            // Here remove the specific n column, n = csvLine.size() - schema.length
                            int numberOfRemovedFromStart = csvLine.size() - avroSchemaSize;
                            String[] columns = new String[avroSchemaSize];

                            for (int i = 1, j = numberOfRemovedFromStart; i <= avroSchemaSize && j < csvLine.size();
                                 i++, j++) {
                                columns[i - 1] = csvLine.get(j);
                            }

                            return RowFactory.create(DFSchemaUtility.structDecodingFromLine(columns, schemaString));
                        }
                );

        SQLContext sqlContext = new SQLContext(javaSparkContext);

        StructType schema = DFSchemaUtility.structTypeForSchema(avroSchema);

        sqlContext.createDataFrame(rowRDD, schema)
                .coalesce(1)
                .write()
                .mode(AVRO_FILE_WRITE_MODE)
                .format("com.databricks.spark.avro")
                .save(avroPath)
        ;

    }
}
