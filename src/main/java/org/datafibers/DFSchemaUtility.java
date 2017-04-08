package org.datafibers;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.List;

/**
 * Spark Utilities for Avro data
 *
 */
public class DFSchemaUtility {

    /**
     * The equivalent Spark SQL schema for the given Avro schema.
     * @param schema
     * @return Spark SQL structType schema
     */
    public static StructType structTypeForSchema(Schema schema) {
        List<StructField> fields = Lists.newArrayList();

        for (Schema.Field field : schema.getFields()) {
            Schema.Type fieldType = field.schema().getType();

            if (fieldType.equals(Schema.Type.UNION)) {
                fieldType = field.schema().getTypes().get(1).getType();
            }

            switch (fieldType) {
                case STRING:
                    fields.add(DataTypes.createStructField(field.name(), DataTypes.StringType, true));
                    break;
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
     *
     * @param line
     * @param dataSchemaString
     * @return array of object
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
                        case STRING:
                            value = line[index];
                            break;
                        case DOUBLE:
                            value = line[index];
                            break;
                        case FLOAT:
                            value = line[index];
                            break;
                        case INT:
                            value = Integer.parseInt(line[index]);
                            break;
                        case LONG:
                            value = Long.parseLong(line[index]);
                            break;
                        case BOOLEAN:
                            value = Boolean.parseBoolean(line[index]);
                            break;
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

}
