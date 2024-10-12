package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class SqsSinkWriteBuilder implements WriteBuilder {

    private final SqsSinkOptions options;
    private final StructType schema;

    public SqsSinkWriteBuilder(SqsSinkOptions options, StructType schema)
    {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public Write build() {
        return new SqsSinkWrite(this.options, this.schema);

    }

}
