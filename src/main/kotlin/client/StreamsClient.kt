package client

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient

class StreamsClient(val dynamoDbClient: DynamoDbClient, private val dynamoDbStreamsClient: DynamoDbStreamsClient, val streamArn: String) {
  suspend fun start() {
    ShardConsumerController(dynamoDbStreamsClient)
      .consumeStream(streamArn) { println(it.dynamodb?.keys) }
  }
}


