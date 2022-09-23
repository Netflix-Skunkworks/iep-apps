## Introduction

Describes the minimum configuration required for the IAM role used by the Atlas Slotting Service.

## Role

```bash
aws --profile $PROFILE_NAME iam get-role --role-name $ROLE_NAME
```

```json
{
  "Role": {
    "Path": "/",
    "RoleName": "$ROLE_NAME",
    "RoleId": "$ROLE_ID",
    "Arn": "arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME",
    "CreateDate": "$CREATE_DATE",
    "AssumeRolePolicyDocument": {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "Service": "ec2.amazonaws.com"
          },
          "Action": "sts:AssumeRole"
        }
      ]
    },
    "MaxSessionDuration": 3600
  }
}
```

## Role Policies

```bash
aws --profile $PROFILE_NAME iam list-role-policies --role-name $ROLE_NAME
```

```json
{
  "PolicyNames": [
    "AutoScaling",
    "DynamoDB",
    "S3"
  ]
}
```

### AutoScaling

Allow the instance to use AutoScaling DescribeAutoScalingGroups calls for any ASG in the account.

```bash
aws --profile $PROFILE_NAME iam get-role-policy --role-name $ROLE_NAME --policy-name AutoScaling
```

```json
{
  "RoleName": "$ROLE_NAME",
  "PolicyName": "AutoScaling",
  "PolicyDocument": {
    "Statement": [
      {
        "Action": [
          "autoscaling:DescribeAutoScalingGroups"
        ],
        "Effect": "Allow",
        "Resource": [
          "*"
        ]
      }
    ]
  }
}
```

### DynamoDB

Allow the instance full access to the DynamoDB tables it will operate in each region.

```bash
aws --profile $PROFILE_NAME iam get-role-policy --role-name $ROLE_NAME --policy-name DynamoDB
```

```json
{
    "RoleName": "$ROLE_NAME",
    "PolicyName": "DynamoDB",
    "PolicyDocument": {
        "Statement": [
            {
                "Action": [
                    "dynamodb:*"
                ],
                "Effect": "Allow",
                "Resource": [
                    "arn:aws:dynamodb:*:$ACCOUNT_ID:table/atlas_slotting*"
                ]
            }
        ]
    }
}
```

### EC2

Allow the instance to use EC2 DescribeInstances calls for any instance in the account.

```bash
aws --profile $PROFILE_NAME iam get-role-policy --role-name $ROLE_NAME --policy-name EC2
```

```json
{
  "RoleName": "$ROLE_NAME",
  "PolicyName": "EC2",
  "PolicyDocument": {
    "Statement": [
      {
        "Action": [
          "ec2:DescribeInstances"
        ],
        "Effect": "Allow",
        "Resource": [
          "*"
        ]
      }
    ]
  }
}
```

### S3

Allow the instance to write log files to S3 buckets.

```bash
aws --profile $PROFILE_NAME iam get-role-policy --role-name $ROLE_NAME --policy-name S3
```

```json
{
    "RoleName": "$ROLE_NAME",
    "PolicyName": "S3",
    "PolicyDocument": {
        "Statement": [
            {
                "Action": [
                    "s3:ListBucket"
                ],
                "Effect": "Allow",
                "Resource": [
                    "arn:aws:s3:::$BUCKET_NAME"
                ]
            },
            {
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Effect": "Allow",
                "Resource": [
                    "arn:aws:s3:::$BUCKET_NAME/*"
                ]
            }
        ]
    }
}
```
