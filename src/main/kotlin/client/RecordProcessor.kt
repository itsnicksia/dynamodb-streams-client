package client

import aws.sdk.kotlin.services.dynamodbstreams.model.Record

fun interface RecordProcessor {
    fun process(record: Record)
}