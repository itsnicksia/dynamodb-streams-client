package io.fasterthoughts

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import dataTable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

suspend fun ensureTestData(client: DynamoDbClient) {
  println("Creating table [${dataTable}]")

  client.createTable(CreateTableRequest {
    tableName = dataTable
    attributeDefinitions = listOf(
      AttributeDefinition {
        attributeName = "PlayerId"
        attributeType = ScalarAttributeType.fromValue("N")
      },
      AttributeDefinition {
        attributeName = "Sequence"
        attributeType = ScalarAttributeType.fromValue("N")
      }
    )
    keySchema = listOf(
      KeySchemaElement {
        attributeName = "PlayerId"
        keyType = KeyType.Hash
      },
      KeySchemaElement {
        attributeName = "Sequence"
        keyType = KeyType.Range
      }
    )
    provisionedThroughput {
      readCapacityUnits = 10
      writeCapacityUnits = 10
    }
    streamSpecification = StreamSpecification {
      streamEnabled = true
      streamViewType = StreamViewType.NewImage
    }
  })

  println("Adding test data to table [${dataTable}]")
  for (playerId in 0 until 1_000) {
        for (batch in 0 until 100) {
          client.batchWriteItem(BatchWriteItemRequest {
            requestItems = mapOf(
              dataTable to (0 until 10).map {
                WriteRequest {
                  putRequest = PutRequest {
                    item = mapOf(
                      "PlayerId" to AttributeValue.N(playerId.toString()),
                      "Sequence" to AttributeValue.N(it.toString())
                    )
                  }
                }
              }
            )
          })
        }
    println("Finished adding test data for playerId=$playerId")
    }
}