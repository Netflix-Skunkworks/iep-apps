```text
iep.lwc.fwd.cw.ExpressionDetails {

    // use DataSourceId
    partition key: ExpressionId

    // Config key and expression
    ExpressionId: string

    // report timestamp
    LastReportTs: number

    // FwdMetricInfo
    FwdMetricInfo: document

    // Throwable
    Error: string

    // if (no data) reportTs - lastReportTs
    NoDataAgeMins: number

    // if (no scaling policy) reportTs - lastReportTs
    NoScalingPolicyAgeMins: number

    // scaling policy
    ScalingPolicy: document
}
```