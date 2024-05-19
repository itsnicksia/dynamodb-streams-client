package client

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient
import aws.sdk.kotlin.services.dynamodbstreams.model.DescribeStreamRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.Shard
import java.util.*

class StreamsClient(val dynamoDbClient: DynamoDbClient, val dynamoDbStreamsClient: DynamoDbStreamsClient, val streamArn: String) {
  suspend fun start() {

    val shardMap = buildOrderedShardsMap();

//    coroutineScope {
//      shards.forEach { shard ->
//        launch {
//          ShardClient(shard).start()
//        }
//      }
//    }
  }

  /**
   * Create a map of ancestor shards to queues containing descendant shards ordered by sequence number.
   *
   * This allows consumers to quickly grab the next shard to process while maintaining partition order.
   */
  private suspend fun buildOrderedShardsMap(): MutableMap<String, PriorityQueue<ShardData>> {
    val shardMap = mutableMapOf<String, PriorityQueue<ShardData>>()
    val ancestorForest = DirectedForest<String>()

    var lastFetchedShardId: String? = null

    do {
      val streamDescription = dynamoDbStreamsClient.describeStream(DescribeStreamRequest {
        streamArn = this@StreamsClient.streamArn
        exclusiveStartShardId = lastFetchedShardId
      }).streamDescription

      requireNotNull(streamDescription) { "streamDescription cannot be null" }

      /**
       * Map the Shard class from DynamoDB library to something that's easier to work with (i.e. enforcing not-null, removing extraneous fields).
       */
      val shards = requireNotNull(streamDescription.shards) { "streamDescription has no shards" }
      val shardData = shards.map(ShardData::fromShard)

      /**
       * Update the ancestry graph with the new shards
       */
      ancestorForest.addNodes(shardData.map { ancestorForest.RawNode(it.shardId, it.parentShardId) })

      /**
       * Add each shard to the corresponding priority queue.
       */
      shardData.forEach { shard ->
        val ancestorShardId = ancestorForest.getRootId(shard.shardId)
        val isAncestor = ancestorShardId == null

        if (isAncestor) {
          require(!shardMap.containsKey(shard.shardId)) { "Unable to add ancestor shard when ancestor already exists (shardId=${shard.shardId})" }

          /**
           * Create queue with ancestor shard as first element.
           */
          shardMap[shard.shardId] = PriorityQueue<ShardData>(100, compareBy { it.startingSequenceNumber })
        }

        val shardQueue = requireNotNull(shardMap[ancestorShardId]) { "Unable to find shard queue in shard map for shardId=$ancestorShardId" }
        shardQueue.add(shard)
      }

      lastFetchedShardId = streamDescription.lastEvaluatedShardId
    } while (lastFetchedShardId != null)

    return shardMap
  }

  data class ShardData(val shardId: String, val parentShardId: String?, val startingSequenceNumber: Int) {
    companion object {
      fun fromShard(shard: Shard): ShardData {
        val shardId = shard.shardId
        val parentShardId = shard.parentShardId
        val startingSequenceNumber = shard.sequenceNumberRange?.startingSequenceNumber

        requireNotNull(shardId) { "shardId cannot be null" }
        requireNotNull(parentShardId) { "parentShardId cannot be null" }
        requireNotNull(startingSequenceNumber) { "parentShardId cannot be null" }

        return ShardData(
          shardId = shardId,
          parentShardId = parentShardId,
          startingSequenceNumber = startingSequenceNumber.toInt()
        )
      }
    }
  }
}


