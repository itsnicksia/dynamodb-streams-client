package client

import adapters.streams.StreamsApiClient
import aws.sdk.kotlin.services.dynamodbstreams.model.Record
import kotlinx.coroutines.channels.Channel
import kotlin.time.Duration.Companion.seconds

val BACKOFF_DELAY = 5.seconds

/*
  Only one shard client should be active per parent shard to maintain partition order.
  A shard iterator expires 15 minutes after it is returned to the requester.

  Links:
   - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html
   - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html
 */
class ShardReader(private val streamArn: String, private val shardId: String, private val streamsApiClient: StreamsApiClient) {
  suspend fun readRecordsTo(channel: Channel<Record>):Boolean {
    var shardIterator: String? = streamsApiClient.getShardIterator(streamArn, shardId = shardId, loadLastSequenceNumberProcessed())
    println("shardIterator=$shardIterator")

    do {
      val getRecordsResponse = streamsApiClient.getRecords(shardIterator!!, GET_RECORDS_BATCH_SIZE)
      getRecordsResponse.records.forEach { channel.send(it) }
      shardIterator = getRecordsResponse.nextShardIterator
    } while (shardIterator != null)

    println("Finished consuming shard. [shardId=$shardId]")
    return true
  }

  private fun loadLastSequenceNumberProcessed(): String? {
    return null
  }


}