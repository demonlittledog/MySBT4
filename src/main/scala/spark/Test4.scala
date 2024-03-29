package spark


object Test4 {
  def main(args: Array[String]): Unit = {
    def filter(xs: List[Int], threshold: Int) = {
      def process(ys: List[Int]): List[Int] =
        if (ys.isEmpty)
          ys
        else if (ys.head < threshold)
          ys.head :: process(ys.tail)
        else
          process(ys.tail)
      process(xs)   }
    println(filter(List(3, 9, 1, 8, 2, 7, 4), 5))

    abstract class SemiGroup[A] {
      def add(x: A, y: A): A
    }
    abstract class Monoid[A] extends SemiGroup[A] {
      def unit: A
    }
    implicit object StringMonoid extends Monoid[String] {
      def add(x: String, y: String): String = x concat y
      def unit: String = ""
    }
    implicit object IntMonoid extends Monoid[Int] {
      def add(x: Int, y: Int): Int = x + y
      def unit: Int = 0
    }
    def sum[A](xs: List[A])(implicit m: Monoid[A]): A = {
      if (xs.isEmpty) m.unit
      else m.add(xs.head, sum(xs.tail))
    }
    println(sum(List(1, 2, 3)))
    println(sum(List("a", "b", "c")))

    def fun = {   println("call")   }
    val v = fun
    val f = fun _

  }

}
