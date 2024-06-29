package client

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient

class DynamoDbLeaseManager(dynamoDbClient: DynamoDbClient) {
    init {
        // TODO: ensure lease schema exists.
        // TODO: generate lease manager id
    }
    fun takeLease() {}
    fun saveLastProcessedSequenceNumber() {}
}