package org.sparkexample;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

public class SparkWholeFilesAvroProducer implements Serializable {

    public static void main(String[] args) {
        // Run this only in Windows
        if (System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
            System.setProperty("hadoop.home.dir", Paths.get("src/main/resources").toAbsolutePath().toString());

        SparkWholeFilesAvroProducer wholeTextFiles = new SparkWholeFilesAvroProducer();
        wholeTextFiles.run(
                "|",
                Paths.get("src/main/resources/data").toAbsolutePath().toString(),
                Paths.get("src/main/resources/schema", "twitter.avsc").toAbsolutePath().toString(),
                Paths.get("src/main/resources/avro").toAbsolutePath().toString(),
                Paths.get("src/main/resources/temp", UUID.randomUUID().toString()).toAbsolutePath().toString()
        );
    }

    private void run(String delimiter, String path, String schemaPath, String avroPath, String tempFolder) {


        try {
            final Schema avroSchema = new Schema.Parser().parse(Files.toString(new File(schemaPath), Charsets.UTF_8));

            SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("whole text files");
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
            JavaPairRDD<String, String> fileNameContentsRDD =
                    javaSparkContext.wholeTextFiles(path);
            JavaRDD<String> content = fileNameContentsRDD.map(new Function<Tuple2<String, String>, String>() {
                @Override
                public String call(Tuple2<String, String> fileNameContent) throws Exception {
                    String content = fileNameContent._2();
                    ArrayList<String> contentArrayList = new ArrayList<>(Arrays.asList(
                            content.split(System.getProperty("line.separator"))
                    ));
                    contentArrayList.remove(0);
                    contentArrayList.remove(0);
                    contentArrayList.remove(contentArrayList.size() - 1);
                    return String.join(System.getProperty("line.separator"), contentArrayList);
                }
            });

            content.saveAsTextFile(tempFolder);

            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(javaSparkContext);

            StructType schema = SparkEngineUtility.structTypeForSchema(avroSchema); //a.createStructType(fields);

            sqlContext.read()
                    .format("com.databricks.spark.csv")
                    .option("delimiter", delimiter)
                    .schema(schema)
                    .load(tempFolder)
                    .coalesce(1)
                    .write()
                    .mode("overwrite")
                    .format("com.databricks.spark.csv")
                    .save(avroPath)
            ;

            FileUtils.deleteDirectory(new File(tempFolder)); //TODO delete by HDFS or oozie

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }


    }
}
