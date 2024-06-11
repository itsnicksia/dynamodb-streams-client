package client

import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient
import aws.sdk.kotlin.services.dynamodbstreams.model.GetRecordsRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.GetShardIteratorRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.ShardIteratorType
import kotlinx.coroutines.channels.Channel

const val GET_RECORDS_BATCH_SIZE = 1000

/*
  Only one shard client should be active per parent shard to maintain partition order.
  A shard iterator expires 15 minutes after it is returned to the requester.

  Links:
   - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html
 */
class ShardConsumer(val streamArn: String, val shardId: String, private val streamsClient: DynamoDbStreamsClient) {
  private val inputChannel: Channel<Record> = Channel(GET_RECORDS_BATCH_SIZE)
  suspend fun startProcessing(recordProcessor: RecordProcessor) {
    var previousSequenceNumber: String? = null
    //loadBookmark()

    while (true) {
      val shardIterator = getShardIterator(streamArn, shardId = shardId, previousSequenceNumber)
      println("shardIterator=$shardIterator")
      val records = streamsClient.getRecords(GetRecordsRequest {
        this.shardIterator = shardIterator
        this.limit = GET_RECORDS_BATCH_SIZE
      }).records!!

      records.forEach(recordProcessor::process)
    }

    //saveBookmark()
  }

  private suspend fun getShardIterator(streamArn: String, shardId: String, previousSequenceNumber: String?): String {
    val shardIterator = streamsClient.getShardIterator(GetShardIteratorRequest {
      this.streamArn = streamArn
      this.shardId = shardId
      this.sequenceNumber = previousSequenceNumber
      this.shardIteratorType = when(previousSequenceNumber) {
        /**
         * TRIM_HORIZON - Start reading at the last (untrimmed) stream record, which is the oldest record in the shard.
         * In DynamoDB Streams, there is a 24 hour limit on data retention.
         * Stream records whose age exceeds this limit are subject to removal (trimming) from the stream.
         */
        null -> ShardIteratorType.TrimHorizon

        /**
         * AFTER_SEQUENCE_NUMBER - Start reading right after the position denoted by a specific sequence number.
         */
        else -> ShardIteratorType.AfterSequenceNumber
      }
    }).shardIterator

    requireNotNull(shardIterator) { "shardIterator must be non-null." }

    return shardIterator
  }
}