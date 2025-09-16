package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataTypes;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

import java.util.*;

public class SqsSinkParsers {

    private SqsSinkParsers() {
        throw new IllegalStateException("Utility class");
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends Throwable>[] parseExceptions(Set<String> classesStr) {

        if (classesStr == null || classesStr.isEmpty()) {
            return new Class[0];
        }

        List<Class<? extends Throwable>> list = new ArrayList<>();

        for (String raw : classesStr) {

            String name = raw.trim();

            if (name.isEmpty()) continue;

            try {
                Class<?> c = Class.forName(name);
                if (!Throwable.class.isAssignableFrom(c)) {
                    throw new IllegalArgumentException("Class is not an exception: " + name);
                }
                list.add((Class<? extends Throwable>) c);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Class not found: " + name, e);
            }

        }

        return list.toArray(new Class[0]);

    }

    public static Map<String, MessageAttributeValue> parseMapMessageAttributes(MapData msgAttributesMapData){

        Map<String, MessageAttributeValue> attributes = new HashMap<>();

        msgAttributesMapData.foreach(DataTypes.StringType, DataTypes.StringType, (key, value) -> {
            attributes.put(key.toString(), MessageAttributeValue.builder().dataType("String").stringValue(value.toString()).build());
            return null;
        });

        return attributes;

    }

    public static Map<String, MessageSystemAttributeValue> parseMapMessageSystemAttributes(MapData msgAttributesMapData){

        Map<String, MessageSystemAttributeValue> attributes = new HashMap<>();

        msgAttributesMapData.foreach(DataTypes.StringType, DataTypes.StringType, (key, value) -> {
            attributes.put(key.toString(), MessageSystemAttributeValue.builder().dataType("String").stringValue(value.toString()).build());
            return null;
        });

        return attributes;

    }

}
