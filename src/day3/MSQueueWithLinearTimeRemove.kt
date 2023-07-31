@file:Suppress("FoldInitializerAndIfToElvis", "DuplicatedCode")

package day3

import kotlinx.atomicfu.*

class MSQueueWithLinearTimeRemove<E> : QueueWithRemove<E> {
    private val head: AtomicRef<Node>
    private val tail: AtomicRef<Node>

    init {
        val dummy = Node(null)
        head = atomic(dummy)
        tail = atomic(dummy)
    }

    override fun enqueue(element: E) {
        while (true) {
            val curTail = tail.value
            val node = Node(element)
            if (curTail.next.compareAndSet(null, node)) {
                moveTail(curTail)
                if (curTail.extractedOrRemoved) {
                    curTail.remove()
                }
                return
            } else {
                moveTail(curTail)
            }
        }
    }

    private fun moveTail(curTail : Node) {
        val next = tail.value.next.value ?: return
        tail.compareAndSet(curTail, next)
        if (curTail.extractedOrRemoved) {
            curTail.remove()
        }
    }

    override fun dequeue(): E? {
        var curNodeValue = head.value
        while (true) {
            val curNode = curNodeValue.next
            curNodeValue = curNode.value ?: return null
            val result = curNodeValue.element
            if (curNodeValue.markExtractedOrRemoved()) {
                curNodeValue.remove()
                return result
            } else {
                curNodeValue.remove()
            }
        }
    }

    override fun remove(element: E): Boolean {
        // Traverse the linked list, searching the specified
        // element. Try to remove the corresponding node if found.
        // DO NOT CHANGE THIS CODE.
        var node = head.value
        while (true) {
            val next = node.next.value
            if (next == null) return false
            node = next
            if (node.element == element && node.remove()) return true
        }
    }

    /**
     * This is an internal function for tests.
     * DO NOT CHANGE THIS CODE.
     */
    override fun checkNoRemovedElements() {
        check(tail.value.next.value == null) {
            "tail.next must be null"
        }
        var node = head.value
        // Traverse the linked list
        while (true) {
            if (node !== head.value && node !== tail.value) {
                check(!node.extractedOrRemoved) {
                    "Removed node with element ${node.element} found in the middle of this queue"
                }
            }
            node = node.next.value ?: break
        }
    }

    /**
     * This is a string representation of the data structure,
     * you see it in Lincheck tests when they fail.
     * DO NOT CHANGE THIS CODE.
     */
    override fun toString(): String {
        // Choose the leftmost node.
        var node = head.value
        if (tail.value.next.value === node) {
            node = tail.value
        }
        // Traverse the linked list.
        val nodes = arrayListOf<String>()
        while (true) {
            nodes += (if (head.value === node) "HEAD = " else "") +
                    (if (tail.value === node) "TAIL = " else "") +
                    "<${node.element}" +
                    (if (node.extractedOrRemoved) ", extractedOrRemoved" else "") +
                    ">"
            // Process the next node.
            node = node.next.value ?: break
        }
        return nodes.joinToString(", ")
    }

    // TODO: Node is an inner class for accessing `head` in `remove()`
    private inner class Node(
        var element: E?
    ) {
        val next = atomic<Node?>(null)

        /**
         * TODO: Both [dequeue] and [remove] should mark
         * TODO: nodes as "extracted or removed".
         */
        private val _extractedOrRemoved = atomic(false)
        val extractedOrRemoved get() = _extractedOrRemoved.value

        fun markExtractedOrRemoved(): Boolean = _extractedOrRemoved.compareAndSet(false, true)

        /**
         * Removes this node from the queue structure.
         * Returns `true` if this node was successfully
         * removed, or `false` if it has already been
         * removed by [remove] or extracted by [dequeue].
         */
        fun remove(): Boolean {
            val result = markExtractedOrRemoved()
            if (head.value == this) {
                makeDummy()
                return false
            }
            val curPrev = findPrev() ?: return result
            val curNext = next.value
            if (curNext == null) {
                return result
            }
            if (curPrev.next.compareAndSet(this, curNext)) {
                tail.compareAndSet(this, curNext)
            }
            if (curPrev.extractedOrRemoved) {
                curPrev.remove()
            }
            if (curNext.extractedOrRemoved) {
                curNext.remove()
            }
            return result
        }

        private fun findPrev(): Node? {
            var node: Node = head.value
            while (node.next.value != this) {
                node = node.next.value ?: return null
            }
            return node
        }

        fun makeDummy() {
            element = null
            _extractedOrRemoved.compareAndSet(true, false)
        }
    }
}
