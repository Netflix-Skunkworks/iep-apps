```text
iep.lwc.fwd.cw.ExpressionDetails {

    // use ExpressionId
    partition key: ExpressionId

    // Config key and expression
    ExpressionId: string

    // report timestamp
    Timestamp: number

    // FwdMetricInfo
    FwdMetricInfo: string

    // Throwable
    Error: string

    // name -> timestamp
    Events: Map[string, number]

    // scaling policy
    ScalingPolicy: string
}
```

```
aws dynamodb create-table --region us-east-1 --cli-input-json file://ExpressionDetails.json
```