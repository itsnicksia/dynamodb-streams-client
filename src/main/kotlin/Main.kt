import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.DescribeTableRequest
import aws.sdk.kotlin.services.dynamodbstreams.DynamoDbStreamsClient
import aws.smithy.kotlin.runtime.net.url.Url
import client.StreamsClient
import io.fasterthoughts.setupTestData

const val dataTable = "data"

suspend fun main() {

  val dynamoDbClient = DynamoDbClient {
    region = "local"
    endpointUrl = Url.parse("http://localhost:8000")
  }

  val dynamoDbStreamsClient = DynamoDbStreamsClient {
    region = "local"
    endpointUrl = Url.parse("http://localhost:8000")
  }

  setupTestData(dynamoDbClient);

  val tableData = dynamoDbClient.describeTable(DescribeTableRequest {
    tableName = dataTable;
  })

  val streamsClient = StreamsClient(dynamoDbClient, dynamoDbStreamsClient, tableData.table!!.latestStreamArn!!)
  streamsClient.start()
}
