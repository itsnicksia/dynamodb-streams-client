package client

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient
import aws.sdk.kotlin.services.dynamodbstreams.model.DescribeStreamRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.Record
import aws.sdk.kotlin.services.dynamodbstreams.model.Shard
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach

const val GET_RECORDS_BATCH_SIZE = 1000

class ShardConsumerController(private val streamsClient: DynamoDbStreamsClient, private val client: DynamoDbClient) {
    private val shardConsumers = mutableMapOf<String, ShardReader>()

    suspend fun consumeStream(streamArn: String, onConsumeRecord: (Record) -> Unit) {
        // TODO: Spin until lease acquired!
        // TODO: Map to local Shard type here!
        val consumers = getShards(streamArn).map { ShardReader(streamArn, it.shardId!!, streamsClient, client) }
        val consumersByShardId = consumers.associateBy { it.shardId }.toMap()
        shardConsumers.putAll(consumersByShardId);

        // TODO: You can only start consuming if you have no parent or the parent is finished processing!

        /**
         * Start a coroutine scope for each consumer.
         *
         * One consumer per shard.
         *
         * TODO: Respawn dead consumers.
         * TODO: Tune configuration - is default dispatcher appropriate?
         * TODO: When finished, destroy channel.
         */
        consumers.forEach {
            val supervisorScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            supervisorScope {
                val channel: Channel<Record> = Channel(GET_RECORDS_BATCH_SIZE)
                launch {
                    it.streamRecordsToChannel(channel)
                }

                launch {
                    channel.consumeEach {
                        onConsumeRecord(it)
                        //println("DUMMY: Would have saved $lastSequenceNumberProcessed")
                    }
                    println("exit?")
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
