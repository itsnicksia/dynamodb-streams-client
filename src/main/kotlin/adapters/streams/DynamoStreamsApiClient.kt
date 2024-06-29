package adapters.streams

import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient
import aws.sdk.kotlin.services.dynamodbstreams.model.*
import aws.sdk.kotlin.services.dynamodbstreams.model.Record

data class GetRecordsResponse(val records: List<Record>, val nextShardIterator: String?)

class DynamoStreamsApiClient(private val streamsClient: DynamoDbStreamsClient) : StreamsApiClient {
     override suspend fun getShards(streamArn: String): List<Shard> {
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

    override suspend fun getShardIterator(streamArn: String, shardId: String, nextShardIterator: String?): String {
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
                /**
                 * TRIM_HORIZON - Start reading at the last (untrimmed) stream record, which is the oldest record in the shard.
                 * In DynamoDB Streams, there is a 24-hour limit on data retention.
                 * Stream records whose age exceeds this limit are subject to removal (trimming) from the stream.
                 */
                null -> ShardIteratorType.TrimHorizon

                /**
                 * AT_SEQUENCE_NUMBER - Start reading exactly from the position denoted by a specific sequence number.
                 */

                /**
                 * AT_SEQUENCE_NUMBER - Start reading exactly from the position denoted by a specific sequence number.
                 */
                else -> ShardIteratorType.AtSequenceNumber
            }
        }).shardIterator

        requireNotNull(shardIterator) { "shardIterator must be non-null." }

        return shardIterator
    }

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
    override suspend fun getRecords(shardIterator: String, limit: Int): GetRecordsResponse {
        val getRecordsResponse = streamsClient.getRecords(GetRecordsRequest {
            this.shardIterator = shardIterator
            this.limit = limit
        })

        /**
         * NextShardIterator
         * The next position in the shard from which to start sequentially reading stream records.
         *
         * If set to null, the shard has been closed and the requested iterator will not return any more data.
         */
        val nextShardIterator = getRecordsResponse.nextShardIterator
        println("nextShardIterator=$nextShardIterator")

        val records = getRecordsResponse.records
        requireNotNull(records) { "records must not be null." }

        return GetRecordsResponse(records, nextShardIterator)
    }
}