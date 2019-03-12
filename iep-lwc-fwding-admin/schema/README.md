```text
iep.lwc.fwd.cw.ExpressionDetails {

    // use ExpressionId
    partition key: ExpressionId

    // Config key and expression
    ExpressionId: string

    // report timestamp
    timestamp: number

    // FwdMetricInfo
    FwdMetricInfo: document

    // Throwable
    Error: string

    // name -> timestamp
    events: Map[string, number]

    // scaling policy
    ScalingPolicy: document
}
```

```
aws dynamodb create-table --region us-east-1 --cli-input-json file://ExpressionDetails.json
```