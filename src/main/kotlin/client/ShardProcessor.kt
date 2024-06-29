package client

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import aws.sdk.kotlin.services.dynamodbstreams.model.Record
import kotlinx.coroutines.launch

const val GET_RECORDS_BATCH_SIZE = 1000
const val MAX_PRODUCER_RETRIES = 5
const val MAX_CONSUMER_RETRIES = 5

class ShardProcessor(
    val producer: ShardReader,
    val consumerFunction: (Record) -> Unit,
    val leaseManager: DynamoDbLeaseManager
) {
    private val children: MutableList<ShardProcessor> = mutableListOf()

    fun start() {
        /**
         * Start a coroutine scope for each consumer.
         *
         * One consumer per shard.
         *
         * TODO: When finished, destroy channel.
         * TODO: You can only start consuming if you have no parent or the parent is finished processing!
         */
        val consumerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        consumerScope.apply {
            val channel: Channel<Record> = Channel(GET_RECORDS_BATCH_SIZE)

            launch { produceRecords(producer, channel) }
            launch { consumeRecords(channel) }

            // TODO: Close channel when both producer and consumer for the shard are finished.
        }

        startChildren()
    }

    fun addChild(shardProcessor: ShardProcessor) {
        children.add(shardProcessor)
    }

    private suspend fun produceRecords(shardReader: ShardReader, channel: Channel<Record>) {
        var remainingProducerRetries = MAX_PRODUCER_RETRIES
        while (remainingProducerRetries > 0) {
            try {
                val finished = shardReader.readRecordsTo(channel)
                if (finished) { return; }
            } catch (e: Exception) {
                remainingProducerRetries--
            }
        }
    }

    private suspend fun consumeRecords(channel: Channel<Record>) {
        var remainingConsumerRetries = MAX_CONSUMER_RETRIES
        while (remainingConsumerRetries > 0) {

            try {
                channel.consumeEach {
                    this.consumerFunction(it)
                    // TODO: Batching
                    // FIXME: Wait until we get the lease
                    leaseManager.takeLease()
                    this.leaseManager.saveLastProcessedSequenceNumber(it.dynamodb.sequenceNumber)
                }
            } catch (e: Exception) {
                remainingConsumerRetries--
            }
        }
    }

    private fun startChildren() {
        children.forEach { it.start() }
    }
}