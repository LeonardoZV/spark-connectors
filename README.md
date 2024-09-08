# Spark Connectors

[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.leonardozv%3Aspark-connectors-parent&metric=coverage)](https://sonarcloud.io/summary/new_code?id=com.leonardozv%3Aspark-connectors-parent)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.leonardozv%3Aspark-connectors-parent&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=com.leonardozv%3Aspark-connectors-parent)

Custom source and sink providers for Apache Spark.

## Available Spark Connectors

- [AWS DynamoDB Sink](spark-connectors-aws-dynamodb/README.md)
- [AWS SQS Sink](spark-connectors-aws-sqs/README.md)

## Getting Started

#### Minimum requirements ####

To run the connectors you will need **Java 1.8+**.

## Using the Spark Connectors

The recommended way to use the Spark Connectors for Java in your project is to consume it from Maven Central.

#### Importing the BOM ####

To automatically manage module versions (currently all modules have the same version, but this may not always be the case) we recommend you use the [Bill of Materials][bom] import as follows:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.leonardozv</groupId>
            <artifactId>bom</artifactId>
            <version>2.27.21</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

Then individual modules may omit the `version` from their dependency statement:

```xml
<dependencies>
    <dependency>
        <groupId>com.leonardozv</groupId>
        <artifactId>spark-connectors-aws-dynamodb</artifactId>
    </dependency>
    <dependency>
        <groupId>com.leonardozv</groupId>
        <artifactId>spark-connectors-aws-sqs</artifactId>
    </dependency>
</dependencies>
```
#### Individual Connectors ####

Alternatively you can add dependencies for the specific connectors you use only:

```xml
<dependencies>
    <dependency>
        <groupId>com.leonardozv</groupId>
        <artifactId>spark-connectors-aws-dynamodb</artifactId>
        <version>2.27.21</version>
    </dependency>
    <dependency>
        <groupId>com.leonardozv</groupId>
        <artifactId>spark-connectors-aws-sqs</artifactId>
        <version>2.27.21</version>
    </dependency>
</dependencies>
```

#### All Connectors ####

You can import ALL connectors into your project with only one dependency. Please note that it is recommended to only import the modules you need.

```xml
<dependencies>
    <dependency>
        <groupId>com.leonardozv</groupId>
        <artifactId>spark-connectors</artifactId>
        <version>2.27.21</version>
    </dependency>
</dependencies>
```