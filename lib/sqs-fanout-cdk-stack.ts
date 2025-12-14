import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as eventSources from "aws-cdk-lib/aws-lambda-event-sources";
import * as iam from "aws-cdk-lib/aws-iam";

export class SqsFanoutCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // 1. Input bucket (user uploads images here)
    const inputBucket = new s3.Bucket(this, "InputBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // 2. Output bucket (Lambda writes thumbnails here)
    const outputBucket = new s3.Bucket(this, "OutputBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // 3. SNS topic
    const topic = new sns.Topic(this, "ImageUploadTopic");

    // Allow S3 to publish to SNS
    topic.addToResourcePolicy(
      new iam.PolicyStatement({
        principals: [new iam.ServicePrincipal("s3.amazonaws.com")],
        actions: ["SNS:Publish"],
        resources: [topic.topicArn],
        conditions: {
          ArnLike: { "aws:SourceArn": inputBucket.bucketArn },
        },
      })
    );

    // 4. SQS queue (fan-out subscriber)
    const queue = new sqs.Queue(this, "ImageQueue", {
      visibilityTimeout: cdk.Duration.seconds(60),
    });

    // SNS → SQS subscription
    topic.addSubscription(new subs.SqsSubscription(queue));

    // 5. S3 → SNS notification
    inputBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_PUT,
      new s3n.SnsDestination(topic)
    );

    // 6. Lambda function
    const fn = new lambda.Function(this, "ThumbnailLambda", {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/thumbnail"),
      timeout: cdk.Duration.seconds(10),
      memorySize: 512,
      environment: {
        OUT_BUCKET: outputBucket.bucketName,
      },
    });

    // Permissions
    inputBucket.grantRead(fn);
    outputBucket.grantPut(fn);

    // 7. SQS → Lambda trigger
    fn.addEventSource(
      new eventSources.SqsEventSource(queue, {
        batchSize: 1,
      })
    );

    // Outputs
    new cdk.CfnOutput(this, "InputBucketName", {
      value: inputBucket.bucketName,
    });

    new cdk.CfnOutput(this, "OutputBucketName", {
      value: outputBucket.bucketName,
    });
  }
}
