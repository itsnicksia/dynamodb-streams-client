package client

import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient
import aws.sdk.kotlin.services.dynamodbstreams.model.DescribeStreamRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.GetRecordsRequest
import aws.sdk.kotlin.services.dynamodbstreams.model.Shard
import kotlinx.coroutines.channels.Channel

/*
  Only one shard client should be active per parent shard to maintain partition order.
 */
class ShardConsumer(val shard: Shard, private val streamsClient: DynamoDbStreamsClient) {
  private val inputChannel: Channel<Record> = Channel(1000)
  suspend fun startProcessing(recordProcessor: RecordProcessor) {
    loadBookmark()

    val records = streamsClient.getRecords(GetRecordsRequest {
      this.shardIterator = null
      this.limit = 1000
    }).records!!

    recordProcessor.process(records)

    saveBookmark()
  }
}