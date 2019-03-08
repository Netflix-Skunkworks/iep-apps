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

    // if (no scaling policy) reportTs - lastReportTs
    NoScalingPolicyAgeMins: number

    // scaling policy
    ScalingPolicy: document
}
```