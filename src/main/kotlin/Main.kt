package io.fasterthoughts

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.smithy.kotlin.runtime.net.url.Url

fun main() {
  val client = DynamoDbClient {
    endpointUrl = Url.parse("www.dynamodb.com")
  }
  println("Hello World!")
}