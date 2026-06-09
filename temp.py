AWSTemplateFormatVersion: '2010-09-09'
Description: 'Captures ECS events for a specific cluster and routes them to CloudWatch Logs.'

Parameters:
  ClusterName:
    Type: String
    Description: 'The name of the ECS cluster to capture events for.'

Resources:
  ECSEventsLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub "/aws/events/ecs/${ClusterName}"
      RetentionInDays: 30

  EventBridgeLogsResourcePolicy:
    Type: AWS::Logs::ResourcePolicy
    Properties:
      PolicyName: !Sub "${AWS::StackName}-EventBridgeEcsLogsPolicy"
      PolicyDocument: !Sub >
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Principal": {
                "Service": "events.amazonaws.com"
              },
              "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
              ],
              "Resource": "${ECSEventsLogGroup.Arn}:*"
            }
          ]
        }

  ECSEventsRule:
    Type: AWS::Events::Rule
    Properties:
      Name: !Sub "${ClusterName}-ecs-events"
      Description: !Sub "Captures events for the ${ClusterName} ECS cluster to CloudWatch"
      EventPattern:
        source:
          - "aws.ecs"
        detail:
          clusterArn:
            - !Sub "arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:cluster/${ClusterName}"
      State: ENABLED
      Targets:
        - Arn: !GetAtt ECSEventsLogGroup.Arn
          Id: "EcsEventsLogTarget"
