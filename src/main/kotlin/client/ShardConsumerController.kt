package client

import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient
import aws.sdk.kotlin.services.dynamodbstreams.model.DescribeStreamRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.Shard
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

class ShardConsumerController(private val streamsClient: DynamoDbStreamsClient) {
    private val shardConsumers = mutableMapOf<String, ShardConsumer>()

    suspend fun processStreamArn(streamArn: String, recordProcessor: RecordProcessor) {
        // TODO: Spin until lease acquired!
        // TODO: Map to local Shard type here!
        val consumers = getShards(streamArn).map { ShardConsumer(it, streamsClient, recordProcessor) }
        val consumersByShardId = consumers.associateBy { it.shard.shardId!! }.toMap()
        shardConsumers.putAll(consumersByShardId);
        consumers.forEach { _ ->
            coroutineScope {
                launch {
                    ShardConsumer::startProcessing()
                }
            }
        }
    }

    /**
     * Create a map of ancestor shards to queues containing descendant shards ordered by sequence number.
     *
     * This allows consumers to quickly grab the next shard to process while maintaining partition order.
     */
    private suspend fun getShards(streamArn: String): List<Shard> {
        val shards = mutableListOf<Shard>()

        var lastFetchedShardId: String? = null

        do {
            val streamDescription = streamsClient.describeStream(DescribeStreamRequest {
                this.streamArn = streamArn
                this.exclusiveStartShardId = lastFetchedShardId
            }).streamDescription

            requireNotNull(streamDescription) { "streamDescription cannot be null" }
            val newShards = requireNotNull(streamDescription.shards) { "streamDescription has no shards" }
            shards.addAll(newShards)

            lastFetchedShardId = streamDescription.lastEvaluatedShardId
        } while (lastFetchedShardId != null)

        return shards
    }
}