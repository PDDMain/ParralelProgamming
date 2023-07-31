package day2

import day1.*
import kotlinx.atomicfu.*

class FAABasedQueue<E> : Queue<E> {
    class Node<E>(val index: Int) {
        val array = atomicArrayOfNulls<Any?>(SEGMENT_SIZE)
        val next = atomic<Node<E>?>(null)
    }

    private val enqIdx = atomic(0)
    private val deqIdx = atomic(0)
    private val head: AtomicRef<Node<E>>
    private val tail: AtomicRef<Node<E>>

    init {
        val dummy = Node<E>(0)
        head = atomic(dummy)
        tail = atomic(dummy)
    }

    override fun enqueue(element: E) {
        while (true) {
            val curTail = tail.value
            val i = enqIdx.getAndIncrement()
            val s = findSegment(curTail, i / SEGMENT_SIZE)
            moveTailForward(s)
            if (s.array[i % SEGMENT_SIZE].compareAndSet(null, element)) {
                return
            }
        }
    }

    private fun findSegment(curTail: Node<E>, index: Int): Node<E> {
        var node: Node<E> = curTail
        while (node.index < index) {
            val nextNode = node.next.value
            if (nextNode == null) {
                node = addNode(node)
            }
        }
        require(node.index == index) { "Skipped node" }
        return node
    }

    private fun moveTailForward(s: Node<E>) {
        var curTail = tail.value
        while (s.index < curTail.index) {
            if (!tail.compareAndSet(curTail, s)) {
                curTail = tail.value
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun addNode(node: Node<E>): Node<E> {
        val newNode = Node<E>(node.index + 1)
        node.next.compareAndSet(null, newNode)
        return node.next.value as Node<E>
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (deqIdx.value >= enqIdx.value) return null
            val curHead = head.value
            val i = deqIdx.getAndIncrement()
            val s = findSegment(curHead, i / SEGMENT_SIZE)
            if (!s.array[(i % SEGMENT_SIZE)].compareAndSet(null, POISONED)) {
                return s.array[(i % SEGMENT_SIZE)].value as E
            }
        }
    }

    companion object {
        private val POISONED = Any()
        private const val SEGMENT_SIZE = 16
    }
}
