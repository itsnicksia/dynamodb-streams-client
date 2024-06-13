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
const val MAX_CONSUMER_RETRIES = 5
const val MAX_PRODUCER_RETRIES = 5

class ShardConsumerController(private val streamsClient: DynamoDbStreamsClient, private val client: DynamoDbClient) {
    private val shardConsumers = mutableMapOf<String, ShardReader>()

    suspend fun processStream(streamArn: String, consumerFunction: (Record) -> Unit) {
        val shardReaders = getShards(streamArn).map { ShardReader(streamArn, it.shardId!!, streamsClient, client) }
        val shardReaderMap = shardReaders.associateBy { it.shardId }.toMap()
        shardConsumers.putAll(shardReaderMap);

        // TODO: You can only start consuming if you have no parent or the parent is finished processing!

        /**
         * Start a coroutine scope for each consumer.
         *
         * One consumer per shard.
         *
         * TODO: When finished, destroy channel.
         */
        shardReaders.forEach { startProcessing(it, consumerFunction) }
    }

    private fun startProcessing(shardReader: ShardReader, consumeFunction: (Record) -> Unit) {
        val consumerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        consumerScope.apply {
            val channel: Channel<Record> = Channel(GET_RECORDS_BATCH_SIZE)
            
            launch { produceRecords(shardReader, channel) }
            launch { consumeRecords(channel, consumeFunction) }
        }
    }

    private suspend fun produceRecords(shardReader: ShardReader, channel: Channel<Record>) {
        var remainingProducerRetries = MAX_PRODUCER_RETRIES
        while (remainingProducerRetries > 0) {
            try {
                shardReader.readRecordsTo(channel)
            } catch (e: Exception) {
                remainingProducerRetries--
            }
        }
    }

    private suspend fun consumeRecords(channel: Channel<Record>, consumerFunction: (Record) -> Unit) {
        var remainingConsumerRetries = MAX_CONSUMER_RETRIES
        while (remainingConsumerRetries > 0) {
            try {
                channel.consumeEach {
                    consumerFunction(it)
                    //println("DUMMY: Would have saved $lastSequenceNumberProcessed")
                }
            } catch (e: Exception) {
                remainingConsumerRetries--
            }

            println("exit?")
        }
    }

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
