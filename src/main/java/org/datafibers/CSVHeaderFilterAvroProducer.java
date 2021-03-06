package org.datafibers;

import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 *  CSVHeaderFilterAvroProducer
 *
 *  Read whole test files as RDD with textFile
 *  Remove header row (1st row) only and completely!
 */
public class CSVHeaderFilterAvroProducer implements Serializable {

    final static String AVRO_FILE_WRITE_MODE = "overwrite"; // Can be append as well

    public static void main(String[] args) {

        CSVHeaderFilterAvroProducer wholeTextFiles = new CSVHeaderFilterAvroProducer();
        wholeTextFiles.run(args[0], args[1], args[2]);
    }


    private void run(String sourcePath, String schemaPath, String avroPath) {
        String appName = "HeaderFilter - " + schemaPath.substring(
                schemaPath.lastIndexOf(System.getProperty("file.separator")) + 1,
                schemaPath.length());

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        String schemaString = String.join("", javaSparkContext.textFile(schemaPath).collect());
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        StructType schema = DFSchemaUtility.structTypeForSchema(avroSchema);

        sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("delimiter", "|")
                .schema(schema)
                .option("header", "true")
                .load(sourcePath)
                .coalesce(1)
                .write()
                .mode(AVRO_FILE_WRITE_MODE)
                .format("com.databricks.spark.avro")
                .save(avroPath);
    }
}
