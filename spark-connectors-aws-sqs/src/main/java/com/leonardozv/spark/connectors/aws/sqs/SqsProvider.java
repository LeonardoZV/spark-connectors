package com.leonardozv.spark.connectors.aws.sqs;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SqsProvider implements TableProvider, DataSourceRegister {

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return new StructType(new StructField[]{ new StructField("value", DataTypes.StringType, true, Metadata.empty()) });
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new SqsTable(schema);
    }

    /* This allows the dataframe to have more columns than expected (or optional columns) */
    @Override
    public boolean supportsExternalMetadata() { return true; }

    @Override
    public String shortName() {
        return "sqs";
    }

}
