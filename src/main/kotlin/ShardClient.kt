package io.fasterthoughts

import aws.sdk.kotlin.services.dynamodbstreams.model.Shard

/*
  Only one shard client should be active per parent shard to maintain partition order.
 */
class ShardClient(val shardId: Shard) {
  suspend fun start(shard: Shard) {
  println ("process shard ${shard.shardId}")
  // TODO: Get all shards for stream and build a TREE PER SHARD.
//    coroutineScope {
//
//      // TODO: Get Shards
//      // TODO: Get Bookmarks
//      // TODO: ITS CLIENTIN TIME
  }
}