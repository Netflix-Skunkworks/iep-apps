## Atlas Persistence
Receive Atlas data points and save them to S3.

#### Java code generation from avro schema
- Avro code generation is a dependent step of sbt compile, so it will be executed by:
```sbt compile``` 
- You can also only run code generation with: ```sbt avroGenerate```
- The generated Java Source Code can be found at: ```atlas-persistence/target/scala-*/src_managed```
