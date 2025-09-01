package com.leonardozv.spark.connectors.aws.dynamodb.write;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DynamoDbSinkParsers {

    private DynamoDbSinkParsers() {
        throw new IllegalStateException("Utility class");
    }

    @SuppressWarnings("unchecked")
    static Class<? extends Throwable>[] parseExceptions(Set<String> classesStr) {

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

}
