package org.sparkexample;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

public class SparkLineFilterAvroProducer implements Serializable {

    public static void main(String[] args) {
        // Run this only in Windows
        if (System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
            System.setProperty("hadoop.home.dir", Paths.get("src/main/resources").toAbsolutePath().toString());

        SparkLineFilterAvroProducer wholeTextFiles = new SparkLineFilterAvroProducer();
        wholeTextFiles.run(
                "\\|",
                2, 1,
                Paths.get("src/main/resources/data").toAbsolutePath().toString(),
                Paths.get("src/main/resources/schema", "twitter.avsc").toAbsolutePath().toString(),
                Paths.get("src/main/resources/avro").toAbsolutePath().toString()
        );
    }

    private void run(String delimiter, int headerRowRemoved, int trailerRowRemoved,
                     String sourcePath, String schemaPath, String avroPath) {

        try {
            final Schema avroSchema = new Schema.Parser().parse(Files.toString(new File(schemaPath), Charsets.UTF_8));

            SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkLineFilterAvroProducer");;

            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
            JavaPairRDD<String, String> fileNameContentsRDD = javaSparkContext.wholeTextFiles(sourcePath);

            JavaRDD<String> contentFiltered = fileNameContentsRDD
                    .flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                        public Iterable<String> call(Tuple2<String, String> file) throws Exception {
                            String content = file._2();
                            ArrayList<String> contentArrayList = new ArrayList<>(Arrays.asList(
                                    content.split(System.getProperty("line.separator"))
                            ));

                            if(headerRowRemoved + trailerRowRemoved < contentArrayList.size()) {
                                for(int i = 0; i < headerRowRemoved; i++) {
                                    contentArrayList.remove(0);
                                }
                                for(int i = 0; i < trailerRowRemoved; i++) {
                                    contentArrayList.remove(contentArrayList.size() - 1);
                                }
                            }

                            String[] simpleArray = new String[contentArrayList.size()];
                            simpleArray = contentArrayList.toArray(simpleArray);
                            return Arrays.asList(contentArrayList.toArray(simpleArray));
                        }
                    });


            JavaRDD<Row> rowRDD = contentFiltered
                    .map(new Function<String, String[]>() {
                        @Override
                        public String[] call(String line) throws Exception {
                            return line.split(delimiter);
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

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    private void run(String delimiter, String sourcePath, String schemaPath, String avroPath) {
        run(delimiter, 0, 0, sourcePath, schemaPath, avroPath);

    }
}
