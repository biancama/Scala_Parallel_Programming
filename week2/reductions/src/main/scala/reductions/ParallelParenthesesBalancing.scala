package reductions

import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balance(chars: Array[Char], acc:Int): Boolean =  (chars, acc) match {
      case (_, a) if (a < 0) => false
      case (c, _) if (c.length == 0) => acc == 0
      case (c, a) =>
        if (c.head == '(') balance(c.tail, a + 1)
        else if (c.head == ')') balance(c.tail, a - 1)
        else  balance(c.tail, a)
      }
      balance(chars, 0)
    }


  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(fromIdx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) =
      (fromIdx until until).foldLeft((arg1, arg2)) {
        case ((a1, a2), i) => if (chars(i) == '(') (a1 + 1, a2)
                              else if (chars(i) == ')' && a1 > 0 ) (a1 - 1, a2)
                              else if (chars(i) == ')') (a1, a2 + 1)
                              else (a1, a2)
      }


    def reduce(from: Int, until: Int) : (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = from + (until - from )/2
        val ((leftOpen, leftClosed), (rightOpen, rightClosed)) = parallel(reduce(from, mid), reduce(mid, until))
        if (leftOpen > rightClosed) {
          // )))((())(( => )))(((
          (leftOpen - rightClosed + rightOpen) -> leftClosed
        } else {
          // )))(()))(( => ))))((
          rightOpen -> (rightClosed - leftOpen + leftClosed)
        }
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!
}
