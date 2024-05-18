const val MAX_ALLOWED_DEPTH = 100

class DirectedForest<Id, Value> {
  private val nodeMap = mutableMapOf<Id, Node>()

  fun addNodes(bareNodes: Iterable<BareNode>) {
    //bareNodes.forEach { flattenTowardsRoot(it, MAX_ALLOWED_DEPTH) }
    //nodeMap +=
  }

  private tailrec fun flattenTowardsRoot(
    node: Node,
    remainingDepth: Int,
    seenNodes: MutableMap<Id, Node> = mutableMapOf()
  ) {
    val childId = node.child.id
    val parent = node.parent

    if (parent == null) {
      return
    }

    check (!seenNodes.containsKey(parent.id)) { "Cyclic dependency detected. TODO: Show cycle" }

    return flattenTowardsRoot(Pair(parent.id, parent.getParent()), remainingDepth - 1, seenNodes.add(node.));
  }

  inner class BareNode (val id: Id, val parentId: Id?, val value: Value)
  inner class Node (val id: Id, val value: Value, val parent: Node?)
}

