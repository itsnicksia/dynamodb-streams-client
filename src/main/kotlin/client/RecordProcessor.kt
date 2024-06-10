package client

import aws.sdk.kotlin.services.dynamodbstreams.model.Record

interface RecordProcessor {
    fun process(records: List<Record>)
}