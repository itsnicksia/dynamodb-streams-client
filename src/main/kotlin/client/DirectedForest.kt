package client

/**
 * A two-level directed forest.
 */
class DirectedForest<Id> {
  private val nodeMap = mutableMapOf<Id, Node>()
  private val parentMemo = mutableMapOf<Id, Id>()

  fun addNodes(rawNodes: Iterable<RawNode>) {
    val parentIdMap = rawNodes.associateBy { it.id }

    rawNodes
      .forEach { node ->
        run {
          check(!nodeMap.containsKey(node.id)) { "Node with id=${node.id} already exists in nodeMap." }
          val rootNode = findRootNode(node.id)
          nodeMap[node.id] = Node(node.id, rootNode)
        }
      }
  }

  private fun findRootNode(
    nodeId: Id,
  ): Node {
    val seenIds: MutableSet<Id> = mutableSetOf()

    tailrec fun findRootNodeIdRecursive(nodeId: Id): Id {
      check(!seenIds.contains(nodeId)) { "Cyclic dependency detected. TODO: Show cycle" }
      seenIds.add(nodeId)

      val parentId = parentMemo[nodeId]

      return if (parentId == null) {
        nodeId
      } else {
        findRootNodeIdRecursive(parentId);
      }
    }

    val rootNodeId = findRootNodeIdRecursive(nodeId)

    /**
     * Update memoization
     */
    seenIds.forEach { id -> parentMemo[id] = rootNodeId }

    return Node(rootNodeId)
  }

  fun getRootId(id: Id): Id? {
    val node = nodeMap[id]

    requireNotNull(node) { "Unable to find node with id=$id" }

    return node.parent?.id
  }

  inner class RawNode (val id: Id, val parentId: Id?)
  inner class Node (val id: Id, val parent: Node? = null)
}
