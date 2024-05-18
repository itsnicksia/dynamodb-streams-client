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
  private suspend fun buildOrderedShardsMap(): MutableMap<String, PriorityQueue<Shard>> {
    val shardMap = mutableMapOf<String, PriorityQueue<Shard>>()
    val ancestry = DirectedForest<Shard, String>()

    var lastFetchedShardId: String? = null

    do {
      val streamDescription = dynamoDbStreamsClient.describeStream(DescribeStreamRequest {
        streamArn = this@StreamsClient.streamArn
        exclusiveStartShardId = lastFetchedShardId
      }).streamDescription

      requireNotNull(streamDescription) { "streamDescription cannot be null." }
      val shards = requireNotNull(streamDescription.shards) { "streamDescription has no shards" }

      /**
       * Update the ancestry graph with the new shards
       */
      ancestry.addNodes(shards.map { shard ->
        DirectedForest.Edge(
          child = DirectedForest.Node(shard.shardId!!, shard),
          parent = shard.parentShardId.let(::DirectedForest.Node<Shard>)
        )})

      /**
       * Add each shard to the corresponding priority queue.
       */

      shards.forEach { shard ->
        val shardId = requireNotNull(shard.shardId)
        val startingSequenceNumber = requireNotNull(shard.sequenceNumberRange?.startingSequenceNumber)

        /**
         * If the shard is an ancestor, we add it as the key for the shard map and initialize it as the first entry of the ordered queue.
         *
         * If the shard is a child, we...
         *
         * We also update the ancestor map so we can easily find a shard's ancestor to add to the queue.
         *
         */
        val isAncestor = shard.parentShardId == null
        if (isAncestor) {
          require(!shardMap.containsKey(shardId)) { "Unable to add ancestor shard when ancestor already exists (shardId=$shardId)" }

          /**
           * Create queue with ancestor shard as first element.
           */
          val newOrderedQueue = PriorityQueue<Shard>(100, compareBy { startingSequenceNumber })
          newOrderedQueue.add(shard)
          shardMap[shardId] = newOrderedQueue
        } else {
          val ancestorShardId = requireNotNull(ancestry[shardId])  { "Unable to locate ancestor for shardId=$shardId"}
          val orderedShardQueue = requireNotNull(shardMap[shardId]) { "Unable to find shard queue in shard map for shardId=$shardId" }


        }
      }

      updateAncestorMap(ancestry)

      lastFetchedShardId = streamDescription.lastEvaluatedShardId
    } while (lastFetchedShardId != null)

    return shardMap
  }

  private fun updateAncestorMap(ancestorMap: MutableMap<String, String>) {

  }
}

