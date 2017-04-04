package org.sparkexample;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.JsonNode;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dadu on 03/04/2017.
 */
public class SparkEngineUtility {

    /**
     * The equivalent Spark SQL schema for the given Avro schema.
     */
    public static StructType structTypeForSchema(Schema schema) {
        List<StructField> fields = Lists.newArrayList();

        for (Schema.Field field : schema.getFields()) {
            Schema.Type fieldType = field.schema().getType();

            if (fieldType.equals(Schema.Type.UNION)) {
                fieldType = field.schema().getTypes().get(1).getType();
            }

            switch (fieldType) {
                case DOUBLE:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.DoubleType, true));
                    break;
                case FLOAT:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.FloatType, true));
                    break;
                case INT:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.IntegerType, true));
                    break;
                case LONG:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.LongType, true));
                    break;
                case STRING:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.StringType, true));
                    break;
                case BOOLEAN:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.BooleanType, true));
                    break;
                default:
                    throw new RuntimeException("Unsupported Avro field type: " + fieldType);
            }
        }

        return DataTypes.createStructType(fields);
    }

    /**
     * Decode Csv to Object[] according to avro schema
     * @param line
     * @return struct of decoded
     */
    public static Object[] structDecodingFromLine(String[] line, String dataSchemaString) {

        Schema dataSchema = new Schema.Parser().parse(dataSchemaString);

        if (line.length > 0) {

            Object[] objectList = new Object[line.length];
            List<Schema.Field> fields = dataSchema.getFields();

            for (int index = 0; index <= line.length - 1; index++) {
                Object value = null;
                Schema.Type schemaType = fields.get(index).schema().getType();
                if (schemaType != null) {
                    switch (schemaType) {
                        case STRING: {
                            value = line[index];
                            break;
                        }
                        case INT: {
                            value = Integer.parseInt(line[index]);
                            break;
                        }
                        case LONG: {
                            value = Long.parseLong(line[index]);
                            break;
                        }
                        case BOOLEAN: {
                            value = Boolean.parseBoolean(line[index]);
                            break;
                        }
                        default:
                            value = line[index];
                    }
                }
                objectList[index] = value;
            }

            return objectList;
        }
        return null;
    }

    public static void main(String[] args) {

        try {
            String schemaContent = Files.toString(
                    new File(
                            Paths.get("src/main/resources/schema", "twitter.avsc").toAbsolutePath().toString()
                    ), Charsets.UTF_8);
            structDecodingFromLine("will|this is comments|231231231".split("\\|"), schemaContent);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }





        String content = "header1\r\nheader2\r\ncontent\r\ncontent\r\ntrailer\r\n";
        ArrayList<String> contentArrayList = new ArrayList<String>(Arrays.asList(content.split("[\r\n]+")));
        contentArrayList.remove(0);
        contentArrayList.remove(0);
        contentArrayList.remove(contentArrayList.size() - 1);
        String test = String.join("\r\n", contentArrayList);
        System.out.println(test);
    }
}
