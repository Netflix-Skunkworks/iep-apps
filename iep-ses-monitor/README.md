
# AWS SES Notification Monitoring Service

> Status: Experimental

Amazon Web Services provides a mechanism to monitor SES sending
activity. This service reads from an SQS queue holding sending
activity notifications.

It then:
* logs the full notification to disk.
* extracts key information from the notification to publish as
  metrics under the name `ses.monitor.notifications`.

For more information see:
* [Monitoring Your Amazon SES Sending Activity](https://docs.aws.amazon.com/ses/latest/DeveloperGuide/monitor-sending-activity.html)
* [Monitoring Using Amazon SES Notifications](https://docs.aws.amazon.com/ses/latest/DeveloperGuide/monitor-sending-using-notifications.html)
* [Amazon SES Notifications Through Amazon SNS](https://docs.aws.amazon.com/ses/latest/DeveloperGuide/notifications-via-sns.html)
