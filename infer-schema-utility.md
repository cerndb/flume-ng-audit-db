# Infer Avro schema utility for Kite sink and JDBC source 

When using Kite as sink, previously you need to create a dataset. In order to create a Kite dataset, you should use the following command:

```
kite-dataset create <dataset_name> -s schema.avsc
```

This command receives an argument which points out a file (schema.avsc) containing the new dataset schema. You can infer the schema from a database table with an utility provided. An use case when this utility can be useful is when using ReliableJdbcAuditEventReader in the source.

Below command should be used for running this utility.

```
bin/infer-avro-schema-from-table -c <Connection URL> -t [<SCHEMA_NAME>.]<TABLE_NAME> -u <USERNSME> -p <PASSWORD> [-help]
 -c <CONNECTION_URL>       URL for connecting to database
 -t [<SCHEMA_NAME>.]<TABLE_NAME>           Table from which schema is inferred
 -u <USERNSME>             User to authenticate against database
 -p <PASSWORD>             User's password
 -dc <DRIVER_FQCN>         Fully qualified class name of JDBC driver (default: oracle.jdbc.driver.OracleDriver)
 -help                     Print help
```

This script adds to classpath all JAR files contained in lib folder, there should be placed the JDBC driver.

You can redirect the output of previous command to a local file and then, use this file for creating a Kite datasest. 

```
bin/infer-avro-schema-from-table -c jdbc:oracle:thin:@itrac13108.cern.ch:10121:IMT -u <user> -p <password> -t ADMIN_EMP > schema.avsc
kite-dataset create ADMIN_EMP_DATASET -s schema.avsc
```