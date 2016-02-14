package nodescala

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }
  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }
  test("all returns a future with a list of values from given list of futures") {
    val futures = List(Future.always(1), Future.always(2), Future.always(3))
    val allFuture = Future.all(futures)
    
    assert(Await.result(allFuture, 1 second) == List(1, 2, 3))
  }
  test("all fails if any future fails") {
    val futures = List(Future.always(1), Future.always(2), Future{2/0})
    
    try {
      Await.result(Future.all(futures), 1 second)
      assert(false)
    } catch {
      case t: Throwable => // ok!
    }
  }
  test("any returns the value of the future that completed first from the given list of futures") {
    val futures = List(Future.never[Int], Future.always(2), Future.never[Int])
    assert(Await.result(Future.any(futures), 1 second) == 2)
  }
  test("any fails if first future fails") {
    val futures = List(Future.never[Int], Future{2/0}, Future.never[Int])
    try {
      Await.result(Future.any(futures), 1 second)
      assert(false)
    } catch {
      case t: Throwable => // ok!
    }
  }
  
  test("now returns result of future if it is completed now") {
    assert(Future.always(5).now === 5)
  }
  
  test("now throws a NoSuchElementException if future is not completed now") {
    try {
      Future.delay(1 second).now
      assert(false)
    } catch {
      case t: NoSuchElementException => //ok!
    }
  }
  
  test("continueWith") {
    val f1 = Future.always(5)
    val f2 = f1.continueWith {
      f => "Your number is " + f.value.get.get
    }
    
    assert(Await.result(f2, 1 second) === "Your number is 5")
  }
  
  test("continueWith2") {
    val f1 = Future.delay(2 seconds)
    val f2 = f1.continueWith { f => "foo"}
    assert(Await.result(f2, 3 second) === "foo")
  }
  
  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }
  test("Server should be stoppable if receives infinite  response") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => Iterator.continually("a")
    }

    // wait until server is really installed
    Thread.sleep(500)

    val webpage = dummy.emit("/testDir", Map("Any" -> List("thing")))
    try {
      // let's wait some time
      Await.result(webpage.loaded.future, 1 second)
      fail("infinite response ended")
    } catch {
      case e: TimeoutException =>
    }

    // stop everything
    dummySubscription.unsubscribe()
    Thread.sleep(500)
    webpage.loaded.future.now // should not get NoSuchElementException
  }

}




