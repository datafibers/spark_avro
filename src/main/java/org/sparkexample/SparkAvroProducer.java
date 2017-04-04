package org.sparkexample;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;

public class SparkAvroProducer implements Serializable {

    public static void main(String[] args) {
        // Run this only in Windows
        if(System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
            System.setProperty("hadoop.home.dir", Paths.get("src/main/resources").toAbsolutePath().toString());

        SparkAvroProducer wholeTextFiles = new SparkAvroProducer();
        wholeTextFiles.run(
                Paths.get("src/main/resources/data").toAbsolutePath().toString(),
                Paths.get("src/main/resources/schema", "twitter.avsc").toAbsolutePath().toString(),
                Paths.get("src/main/resources/avro").toAbsolutePath().toString()
        );
    }

    private void run(String path, String schemaPath, String avroPath) {


        try {
            final Schema avroSchema = new Schema.Parser().parse(Files.toString(new File(schemaPath), Charsets.UTF_8));

            SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("whole text files");

            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
            JavaRDD<String> content =
                    javaSparkContext.textFile(path);

            JavaRDD<String> contentFiltered = content.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String content) throws Exception {
                    return !(content.split("\\|")[0].equalsIgnoreCase("H")
                            || content.split("\\|")[0].equalsIgnoreCase("T"));
                }
            });

            JavaRDD<Row> rowRDD = contentFiltered
                    .map(new Function<String, String[]>() {
                        @Override
                        public String[] call(String line) throws Exception {
                            return line.split("\\|");
                        }
                    })
                    .map(new Function<String[], Row>() {
                        @Override
                        public Row call(String[] line) throws Exception {
                            return RowFactory.create(
                                    SparkEngineUtility.structDecodingFromLine(
                                            line,
                                            Files.toString(new File(schemaPath), Charsets.UTF_8)));
                        }
                    });


            SQLContext sqlContext = new SQLContext(javaSparkContext);

            StructType schema = SparkEngineUtility.structTypeForSchema(avroSchema);

            sqlContext.createDataFrame(rowRDD, schema)
                    .coalesce(1)
                    .write()
                    .mode("overwrite")
                    .format("com.databricks.spark.avro")
                    .save(avroPath)
            ;

        }catch (IOException ioe) {
            ioe.printStackTrace();
        }


    }
}
