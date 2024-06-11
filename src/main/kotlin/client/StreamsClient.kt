package client

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient

class StreamsClient(
  private val dynamoDbClient: DynamoDbClient,
  private val dynamoDbStreamsClient: DynamoDbStreamsClient,
  private val streamArn: String) {
  suspend fun start() {
    ShardConsumerController(dynamoDbStreamsClient, dynamoDbClient)
      .consumeStream(streamArn) {
        println("[consumer] ${it.dynamodb?.sequenceNumber}=${it.dynamodb?.keys}")
      }
  }
}


