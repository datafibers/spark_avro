package org.datafibers;

import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 *  CSVLineFilterAvroProducer
 *
 *  Read whole test file as RDD to index line number in each text files with wholeTextFiles
 *  Remove specific rows index from header or trailer as well as columns completely!
 *  The header and trailer number of rows are specified from headerRowRemoved and trailerRowRemoved
 *  The first n columns are removed by csv line size - schema size
 */
public class CSVLineFilterAvroProducer implements Serializable {

    final static char FILE_DELIMITER = '|';
    final static String FILE_LINE_END = "\n";
    final static String AVRO_FILE_WRITE_MODE = "overwrite"; // Can be append as well

    CSVParser csvParser = null;
    CSVFormat csvFileFormat = CSVFormat.EXCEL.withDelimiter(FILE_DELIMITER).withRecordSeparator(FILE_LINE_END);

    public static void main(String[] args) {

        CSVLineFilterAvroProducer wholeTextFiles = new CSVLineFilterAvroProducer();
        wholeTextFiles.run(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2], args[3], args[4]);
    }


    private void run(int headerRowRemoved, int trailerRowRemoved,
                     String sourcePath, String schemaPath, String avroPath) {

        String appName = "LineFilter - " + schemaPath.substring(
                schemaPath.lastIndexOf(System.getProperty("file.separator")) + 1,
                schemaPath.length());

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        String schemaString = String.join("", javaSparkContext.textFile(schemaPath).collect());
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        int avroSchemaSize = avroSchema.getFields().size();

        JavaPairRDD<String, String> fileNameContentsRDD = javaSparkContext.wholeTextFiles(sourcePath);

        JavaRDD<String> contentFiltered = fileNameContentsRDD
                .flatMap(file -> {
                        String content = file._2();
                        csvParser = new CSVParser(new StringReader(content), csvFileFormat);
                        List<CSVRecord> contentArrayList = csvParser.getRecords();

                        if (headerRowRemoved + trailerRowRemoved < contentArrayList.size()) {
                            for (int i = 0; i < headerRowRemoved; i++) {
                                contentArrayList.remove(0);
                            }
                            for (int i = 0; i < trailerRowRemoved; i++) {
                                contentArrayList.remove(contentArrayList.size() - 1);
                            }
                        }

                        ArrayList<String> stringArrayList = new ArrayList<>();

                        // Dummy copy since we cannot create new CSVRecord
                        for (CSVRecord c : contentArrayList) {
                            StringBuffer stringBuffer = new StringBuffer();
                            for (int i = 0; i < avroSchemaSize; i++) {
                                if (i >= 1) stringBuffer.append(FILE_DELIMITER);
                                stringBuffer.append(c.get(i));
                            }
                            stringArrayList.add(stringBuffer.toString());
                        }
                        return stringArrayList;
                    }
                );


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
                .save(avroPath);
    }

    private void run(String sourcePath, String schemaPath, String avroPath) {
        run(0, 0, sourcePath, schemaPath, avroPath);

    }
}
