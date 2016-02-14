package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, insert(b, empty))
    findMin(h) == List(a, b).min
  }

  property("deleteMin1") = forAll { a: Int =>
    val h = insert(a, empty)
    isEmpty(deleteMin(h))
  }

  property("deleteMin2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, insert(b, empty))
    findMin(deleteMin(h)) == List(a, b).max
  }

  property("minMeld") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == List(findMin(h1), findMin(h2)).min
  }

  property("sorted") = forAll { (h: H) =>
    def getElements(h: H, elements: List[Int]): List[Int] = {
      if (isEmpty(h)) elements
      else getElements(deleteMin(h), elements :+ findMin(h))
    }

    val elements = getElements(h, List())
    elements == elements.sorted
  }

  lazy val genHeap: Gen[H] = for {
    x <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(x, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
