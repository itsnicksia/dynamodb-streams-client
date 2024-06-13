package client

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient
import aws.sdk.kotlin.services.dynamodbstreams.model.GetRecordsRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.GetShardIteratorRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.Record
import aws.sdk.kotlin.services.dynamodbstreams.model.ShardIteratorType
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
class ShardReader(private val streamArn: String, val shardId: String, private val streamsClient: DynamoDbStreamsClient, private val client: DynamoDbClient) {
  suspend fun readRecordsTo(channel: Channel<Record>) {
    /**
     * GetShardIterator
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html
     *
     * Returns a shard iterator. A shard iterator provides information about how to retrieve the stream records from within a shard. Use the shard iterator in a subsequent GetRecords request to read the stream records from the shard.
     */
    var shardIterator: String? = getShardIterator(streamArn, shardId = shardId, loadLastSequenceNumberProcessed())
    println("shardIterator=$shardIterator")

    do {
      /**
       * GetRecords
       * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html
       * Retrieves the stream records from a given shard.
       *
       * Specify a shard iterator using the ShardIterator parameter.
       *
       * The shard iterator specifies the position in the shard from which you want to start reading stream records sequentially.
       * If there are no stream records available in the portion of the shard that the iterator points to, GetRecords returns an empty list.
       * Note that it might take multiple calls to get to a portion of the shard that contains stream records.
       */
      val getRecordsResponse = streamsClient.getRecords(GetRecordsRequest {
        this.shardIterator = shardIterator
        this.limit = GET_RECORDS_BATCH_SIZE
      })

      val records = getRecordsResponse.records
      requireNotNull(records) { "records must not be null." }

      records.forEach { channel.send(it) }

      /**
       * NextShardIterator
       * The next position in the shard from which to start sequentially reading stream records.
       *
       * If set to null, the shard has been closed and the requested iterator will not return any more data.
       */
      val nextShardIterator = getRecordsResponse.nextShardIterator
      println("nextShardIterator=$nextShardIterator")
      shardIterator = nextShardIterator

    } while (shardIterator != null)

    println("Finished consuming shard. [shardId=$shardId]")
  }

  private fun loadLastSequenceNumberProcessed(): String? {
    return null
  }

  private suspend fun getShardIterator(streamArn: String, shardId: String, nextShardIterator: String?): String {
    val shardIterator = streamsClient.getShardIterator(GetShardIteratorRequest {
      this.streamArn = streamArn
      this.shardId = shardId
      this.sequenceNumber = nextShardIterator
      this.shardIteratorType = when(nextShardIterator) {
        /**
         * TRIM_HORIZON - Start reading at the last (untrimmed) stream record, which is the oldest record in the shard.
         * In DynamoDB Streams, there is a 24-hour limit on data retention.
         * Stream records whose age exceeds this limit are subject to removal (trimming) from the stream.
         */
        null -> ShardIteratorType.TrimHorizon

        /**
         * AT_SEQUENCE_NUMBER - Start reading exactly from the position denoted by a specific sequence number.
         */
        else -> ShardIteratorType.AtSequenceNumber
      }
    }).shardIterator

    requireNotNull(shardIterator) { "shardIterator must be non-null." }

    return shardIterator
  }
}