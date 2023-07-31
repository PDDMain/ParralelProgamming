package day1

import kotlinx.atomicfu.*
import java.util.concurrent.*

class TreiberStackWithElimination<E> : Stack<E> {
    private val stack = TreiberStack<E>()

    private val eliminationArray = atomicArrayOfNulls<Any?>(ELIMINATION_ARRAY_SIZE)

    override fun push(element: E) {
        if (tryPushElimination(element)) return
        stack.push(element)
    }

    private fun tryPushElimination(element: E): Boolean {
        val index = randomCellIndex()
        val cell = eliminationArray[index]
        if (cell.compareAndSet(CELL_STATE_EMPTY, element)) {
            repeat(ELIMINATION_WAIT_CYCLES) {
                if (cell.compareAndSet(CELL_STATE_RETRIEVED, CELL_STATE_EMPTY)) {
                    return true
                }
            }
            if (!cell.compareAndSet(element, CELL_STATE_EMPTY)) {
                return cell.compareAndSet(CELL_STATE_RETRIEVED, CELL_STATE_EMPTY)
            }
        }
        return false
    }

    override fun pop(): E? = tryPopElimination() ?: stack.pop()

    @Suppress("UNCHECKED_CAST")
    private fun tryPopElimination(): E? {
        val index = randomCellIndex()
        val cell = eliminationArray[index]
        val element = cell.value ?: return null
        if (element == CELL_STATE_RETRIEVED) {
            return null
        }
        if (cell.compareAndSet(element, CELL_STATE_RETRIEVED)) {
            return element as E?
        }
        return null
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(eliminationArray.size)

    companion object {
        private const val ELIMINATION_ARRAY_SIZE = 2 // Do not change!
        private const val ELIMINATION_WAIT_CYCLES = 1 // Do not change!

        // Initially, all cells are in EMPTY state.
        private val CELL_STATE_EMPTY = null

        // `tryPopElimination()` moves the cell state
        // to `RETRIEVED` if the cell contains element.
        private val CELL_STATE_RETRIEVED = Any()
    }
}