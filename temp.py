AWSTemplateFormatVersion: '2010-09-09'

Parameters:
  LogGroupName:
    Type: String
  AlertEmail1:
    Type: String
    Description: Primary email address (Required)
  AlertEmail2:
    Type: String
    Description: Secondary email address (Optional)
    Default: ""

Conditions:
  HasSecondEmail: !Not [!Equals [!Ref AlertEmail2, ""]]

Resources:
  AlarmSNSTopic:
    Type: AWS::SNS::Topic

  # Primary Email Subscription
  AlarmSubscription1:
    Type: AWS::SNS::Subscription
    Properties:
      TopicArn: !Ref AlarmSNSTopic
      Endpoint: !Ref AlertEmail1
      Protocol: email

  # Optional Secondary Email Subscription
  AlarmSubscription2:
    Condition: HasSecondEmail
    Type: AWS::SNS::Subscription
    Properties:
      TopicArn: !Ref AlarmSNSTopic
      Endpoint: !Ref AlertEmail2
      Protocol: email

  # Uses '?' for OR logic: Matches either exact phrase
  SyncErrorMetricFilter:
    Type: AWS::Logs::MetricFilter
    Properties:
      LogGroupName: !Ref LogGroupName
      FilterPattern: '?"Artifactory Sync - Error" ?"Git Sync - Error"'
      MetricTransformations:
        - MetricName: SyncErrorCount
          MetricNamespace: Custom/SyncErrors
          MetricValue: "1"

  SyncErrorAlarm:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: Sync-Error-Alarm
      MetricName: SyncErrorCount
      Namespace: Custom/SyncErrors
      Statistic: Sum
      Period: 300 
      EvaluationPeriods: 1
      Threshold: 1
      ComparisonOperator: GreaterThanOrEqualToThreshold
      TreatMissingData: notBreaching
      AlarmActions:
        - !Ref AlarmSNSTopic
