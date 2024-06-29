package adapters.streams

import aws.sdk.kotlin.services.dynamodbstreams.model.Shard

interface StreamsApiClient {
    suspend fun getShards(streamArn: String): List<Shard>

    /**
     * GetShardIterator
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html
     *
     * Returns a shard iterator. A shard iterator provides information about how to retrieve the stream records from within a shard. Use the shard iterator in a subsequent GetRecords request to read the stream records from the shard.
     */
    suspend fun getShardIterator(streamArn: String, shardId: String, nextShardIterator: String?): String?

    suspend fun getRecords(shardIterator: String, limit: Int): GetRecordsResponse
}