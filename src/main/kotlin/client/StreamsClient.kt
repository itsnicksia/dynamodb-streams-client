package client

import adapters.streams.StreamsApiClient
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodbstreams.model.Record

class StreamsClient(private val dynamoDbClient: DynamoDbClient, private val streamsApiClient: StreamsApiClient) {
  private val shardProcessors =  mutableMapOf<String, ShardProcessor>()
  private val leaseManager = DynamoDbLeaseManager(dynamoDbClient)

  suspend fun processStream(streamArn: String, consumerFunction: (Record) -> Unit) {
    streamsApiClient
      .getShards(streamArn)
      .forEach {
        val shardId = it.shardId
        val parentShardId = it.parentShardId

        requireNotNull(shardId) { "shardId cannot be null" }

        val producer = ShardReader(streamArn, shardId, streamsApiClient)
        val shardProcessor = ensureShardProcessor(shardId, producer, consumerFunction, leaseManager)

        if (parentShardId != null) {
          val parentShardProcessor = ensureShardProcessor(parentShardId, producer, consumerFunction, leaseManager)
          parentShardProcessor.addChild(shardProcessor)
        } else {
          shardProcessor.start()
        }
      }
  }

  private fun ensureShardProcessor(shardId: String, producer: ShardReader, consumer: (Record) -> Unit, leaseManager: DynamoDbLeaseManager): ShardProcessor {
    val existingShardProcessor = shardProcessors[shardId]

    if (existingShardProcessor != null) {
      return existingShardProcessor
    }

    val shardProcessor = ShardProcessor(producer, consumer, leaseManager)
    shardProcessors[shardId] = shardProcessor
    return shardProcessor
  }
}


