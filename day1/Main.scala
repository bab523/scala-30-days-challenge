object Main {

  // Trait for Task 4
  trait Logger {
    def log(msg: String): Unit
  }

  class ConsoleLogger extends Logger {
    def log(msg: String): Unit = println(s"[Console] $msg")
  }

  class FileLogger extends Logger {
    def log(msg: String): Unit = println(s"[File] $msg (imagine writing to a file)")
  }

  // Scenario class for Task 5
  case class Student(name: String, marks: List[Int]) {
    def average: Double = marks.sum.toDouble / marks.size
  }

  def main(args: Array[String]): Unit = {

    // ---------- Task 1: val, var, lazy val, immutable collections ----------
    println("---- Task 1 ----")
    val fixedName = "Rahul"          // val: cannot change
    var score = 85                    // var: can change
    lazy val heavyCalc = {
      println("Calculating lazily...")
      42
    }
    val marksList: List[Int] = List(80, 90, 70) // immutable collection

    println(s"Name: $fixedName, Score: $score, Marks: $marksList")
    score = 90 // allowed since var
    println(s"Updated score: $score")
    println(s"Lazy val triggered: $heavyCalc") // only now it runs

    // ---------- Task 2: for-comprehension with yield ----------
    println("\n---- Task 2 ----")
    val students = List("Amit", "Priya", "Kiran")
    val marks = List(78, 88, 92)

    val studentMarks = for {
      (student, mark) <- students.zip(marks)
    } yield s"$student scored $mark"

    studentMarks.foreach(println)

    // ---------- Task 3: List, Vector, Set, Map comparison ----------
    println("\n---- Task 3 ----")
    val list1: List[Int] = List(1, 2, 3, 2)
    val vector1: Vector[Int] = Vector(1, 2, 3, 2)
    val set1: Set[Int] = Set(1, 2, 3, 2)
    val map1: Map[String, Int] = Map("a" -> 1, "b" -> 2)

    println(s"List (allows duplicates, ordered): $list1")
    println(s"Vector (fast random access): $vector1")
    println(s"Set (no duplicates): $set1")
    println(s"Map (key-value pairs): $map1")

    // ---------- Task 4: Trait Logger implemented in two classes ----------
    println("\n---- Task 4 ----")
    val consoleLogger: Logger = new ConsoleLogger
    val fileLogger: Logger = new FileLogger
    consoleLogger.log("This is a console log")
    fileLogger.log("This is a file log")

    // ---------- Task 5: Scenario - student grade processor ----------
    println("\n---- Task 5 ----")
    val allStudents = List(
      Student("Amit", List(78, 82, 90)),
      Student("Priya", List(88, 92, 95)),
      Student("Kiran", List(60, 70, 65))
    )

    val results = for {
      s <- allStudents
    } yield s"${s.name} -> Average: ${s.average}"

    results.foreach(println)

    val topStudent = allStudents.maxBy(_.average)
    println(s"\nTop student: ${topStudent.name} with average ${topStudent.average}")
  }
}
