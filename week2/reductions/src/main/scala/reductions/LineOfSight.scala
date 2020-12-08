package reductions

import org.scalameter._

object LineOfSightRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 100,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 10000000
    val input = (0 until length).map(_ % 100 * 1.0f).toArray
    val output = new Array[Float](length + 1)
    val seqtime = standardConfig measure {
      LineOfSight.lineOfSight(input, output)
    }
    println(s"sequential time: $seqtime")

    val partime = standardConfig measure {
      LineOfSight.parLineOfSight(input, output, 10000)
    }
    println(s"parallel time: $partime")
    println(s"speedup: ${seqtime.value / partime.value}")
  }
}

sealed abstract class Tree {
  def maxPrevious: Float
}

case class Node(left: Tree, right: Tree) extends Tree {
  val maxPrevious = left.maxPrevious.max(right.maxPrevious)
}

case class Leaf(from: Int, until: Int, maxPrevious: Float) extends Tree

object LineOfSight extends LineOfSightInterface {
  def maxForNaN(a: Float, b: Float): Float = if (b.isNaN || ( !a.isNaN && a > b )) a else b
  def lineOfSight(input: Array[Float], output: Array[Float]): Unit = {
    input.zipWithIndex.foreach {
      case (_, 0) => output(0) = 0
      case (input, index) => output(index) = maxForNaN(input / index, output(index - 1))
    }
  }


  /** Traverses the specified part of the array and returns the maximum angle.
   */
  def upsweepSequential(input: Array[Float], from: Int, until: Int): Float = {
    (from until until).foldLeft(0f) {
      case (max, index) => maxForNaN(input(index) / index, max)
    }
  }

  /** Traverses the part of the array starting at `from` and until `end`, and
   * returns the reduction tree for that part of the array.
   *
   * The reduction tree is a `Leaf` if the length of the specified part of the
   * array is smaller or equal to `threshold`, and a `Node` otherwise.
   * If the specified part of the array is longer than `threshold`, then the
   * work is divided and done recursively in parallel.
   */
  def upsweep(input: Array[Float], from: Int, end: Int,
              threshold: Int): Tree = {
    if (end - from <= threshold) {
      Leaf(from, end, upsweepSequential(input, from, end))
    } else {
      val mid = from + (end - from) / 2
      val (lL, lR) = parallel(upsweep(input, from, mid, threshold), upsweep(input, mid, end, threshold))
      Node(lL, lR)
    }
  }

  /** Traverses the part of the `input` array starting at `from` and until
   * `until`, and computes the maximum angle for each entry of the output array,
   * given the `startingAngle`.
   */
  def downsweepSequential(input: Array[Float], output: Array[Float],
                          startingAngle: Float, from: Int, until: Int): Unit = {

      def calculateOutput(i: Int, end: Int, max: Float): Unit = {
        if (i < end) {
          output(i) = maxForNaN(input(i) / i, max)
          calculateOutput(i + 1, end, output(i))
        }
      }
    calculateOutput(from, until, startingAngle)
}


  /** Pushes the maximum angle in the prefix of the array to each leaf of the
   *  reduction `tree` in parallel, and then calls `downsweepSequential` to write
   *  the `output` angles.
   */
  def downsweep(input: Array[Float], output: Array[Float], startingAngle: Float,
    tree: Tree): Unit = tree  match {
    case Leaf(from, until, _) => downsweepSequential(input, output, startingAngle, from, until)
    case Node(l, r) => {
      val (_,_) = parallel(downsweep(input, output, startingAngle, l), downsweep(input, output,  maxForNaN(startingAngle, l.maxPrevious), r))
    }
  }

  /** Compute the line-of-sight in parallel. */
  def parLineOfSight(input: Array[Float], output: Array[Float],
    threshold: Int): Unit = {
    val tree = upsweep(input, 0, input.length, threshold)
    downsweep(input, output, 0.0f, tree)
    output(0) = input(0)
  }
}
