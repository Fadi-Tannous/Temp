AWSTemplateFormatVersion: '2010-09-09'

Parameters:
  LogGroupName:
    Type: String
    Description: Name of the existing CloudWatch Log Group to monitor
  AlertEmail:
    Type: String
    Description: Email address to receive the alarm notification

Resources:
  # 1. SNS Topic & Email Subscription
  AlarmSNSTopic:
    Type: AWS::SNS::Topic
    Properties:
      Subscription:
        - Endpoint: !Ref AlertEmail
          Protocol: email

  # 2. Metric Filter to watch for the exact phrase
  ArtifactoryErrorMetricFilter:
    Type: AWS::Logs::MetricFilter
    Properties:
      LogGroupName: !Ref LogGroupName
      FilterPattern: '"Artifactory Sync - Error"'
      MetricTransformations:
        - MetricName: ArtifactorySyncErrorCount
          MetricNamespace: Custom/Artifactory
          MetricValue: "1"

  # 3. CloudWatch Alarm triggered by the metric
  ArtifactoryErrorAlarm:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: Artifactory-Sync-Error-Alarm
      AlarmDescription: "Triggers if 'Artifactory Sync - Error' is found in logs"
      MetricName: ArtifactorySyncErrorCount
      Namespace: Custom/Artifactory
      Statistic: Sum
      Period: 300 # Evaluates every 5 minutes
      EvaluationPeriods: 1
      Threshold: 1 # Alarms on 1 or more errors
      ComparisonOperator: GreaterThanOrEqualToThreshold
      TreatMissingData: notBreaching
      AlarmActions:
        - !Ref AlarmSNSTopic
