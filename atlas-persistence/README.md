## Atlas Persistence
Receive Atlas data points and save them to S3.

#### Java code generation from avro schema
1. Download avro tools jar
2. Run cmd in the directory of atlas-persistence (not root project):
    ```
    java -jar /path/to/avro-tools-1.9.2.jar compile schema ./datapoint.avsc.json src/main/java
    ```