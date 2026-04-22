# <img src="doc/assets/logo.png" height="26"> Wick

A zero cost type safe [Apache Spark](https://spark.apache.org) API.

> Why Wick?

Using Wick gives you the guarantee that your code is correct at compile time, full IDE support with suggestions of
available columns and their types, and better guidance for your AI agents such as Claude.  
Using Wick will save you **HOURS** of work because you won't have to go through lengthy packaging and
deployment to a cluster to test if your job works... repeatedly because it never works the first time :tired_face:

[Demo](https://github.com/user-attachments/assets/ec2e35ea-591a-487e-8df6-5612de7cd642)

> [!TIP]
> Witness Wick in action in the [Leveraging compile-time safety in Spark with Wick](doc/Leveraging_Wick_To_Build_Robust_Data_Pipelines.md)
> post.

Jump to:
* [Getting started](#getting-started)
* [Creating a DataSeq](#creating-a-dataseq)
* [Filtering](#filtering)
* [Selecting](#selecting)
* [Joining](#joining)
* [Aggregating](#aggregating)
* [Ordering/Sorting](#orderingsorting)
* [Column operations](#column-operations)
* [Related projects](#related-projects)
* [Contributing](CONTRIBUTING.md)

(All code snippets in this markdown documentation are compiled and guaranteed to work)


## Getting started

> [!IMPORTANT]
> Wick needs Scala `3.7.0` or higher.  
> This is because Wick relies on [Named Tuples](https://docs.scala-lang.org/scala3/reference/other-new-features/named-tuples.html)
> to provide all these compile time safety and IDE auto-completion.

Add the dependency to Gradle:
```
com.netflix.wick:wick_$scalaBinaryVersion:<version>
```
Or to Scala CLI:
```
com.netflix.wick::wick:<version>
```
Check [the latest version on Maven Central](https://central.sonatype.com/artifact/com.netflix.wick/wick_3/).

It is recommended to enable [Explicit Nulls](https://docs.scala-lang.org/scala3/reference/experimental/explicit-nulls.html)
with the Scala compiler option `-Yexplicit-nulls`. This option allows tracking nullable columns by the type system.

Import everything you need from Wick:
```scala
import com.netflix.wick.{*, given}
```

Create a `SparkSession`:
```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().master("local").getOrCreate()
```

Here is an example you can copy/paste in a file like `wick.scala` and run with
`scala wick.scala` (no prerequirement needed):
```scala
//> using scala 3.7.4
//> using options -Yexplicit-nulls
//> using jvm 17
//> using dep com.netflix.wick::wick:0.0.4
//> using dep org.apache.spark:spark-sql_2.13:3.5.1
//> using javaOptions --add-exports java.base/sun.nio.ch=ALL-UNNAMED

import org.apache.spark.sql.SparkSession
import com.netflix.wick.{*, given} // Import everything you need from Wick

// Write your case class models here (Spark does not like when they are defined inside a def or a class)

@main def wick =
  val spark = SparkSession.builder().master("local").getOrCreate()
  
  // Write your job here
```


## Creating a DataSeq

> [!NOTE]
> `DataSeq` is the type safe representation that Wick introduces instead of the completely untyped `DataFrame` or
> `Dataset` which comes with potential performance drawbacks.
> `DataSeq` is as performant as `DataFrame` can be but with even better typing than `Dataset`.

Given this model:
```scala
case class Department(id: Int, name: String)
case class Employee(id: Int, name: String, dept_id: Int, title_id: Int)
case class Title(id: Int, name: String, managing: Boolean)
```

We can generate some dummy data:
```scala
  val employees = spark.createDataSeq(
    Seq(
      Employee(id = 1, name = "Alice", dept_id = 1, title_id = 2),
      Employee(id = 2, name = "Bob", dept_id = 2, title_id = 1),
      Employee(id = 3, name = "Charlie", dept_id = 1, title_id = 2)
    )
  )
  val departments = spark.createDataSeq(
    Seq(
      Department(id = 1, name = "Engineering"),
      Department(id = 2, name = "Marketing")
    )
  )
  val titles = spark.createDataSeq(
    Seq(
      Title(id = 1, name = "Ad Manager", managing = true),
      Title(id = 2, name = "Software Engineer", managing = false)
    )
  )

  employees.show()
  // +---+-------+-------+--------+
  // | id|   name|dept_id|title_id|
  // +---+-------+-------+--------+
  // |  1|  Alice|      1|       2|
  // |  2|    Bob|      2|       1|
  // |  3|Charlie|      1|       2|
  // +---+-------+-------+--------+
```

Or load the data from actual tables:
```scala ignore
  val employees   = spark.tableDataSeq[Employee]("employees")
  val departments = spark.tableDataSeq[Department]("departments")
  val titles      = spark.tableDataSeq[Title]("titles")
```
:point_up: this will need to run in the cluster (not local) due to ACLs.

> [!NOTE]
> All following examples will assume the above setup (imports and classes).


## Filtering

Wick provides type-safe filtering with compile-time guarantees that your filter conditions are valid:
```scala
  case class Person(name: String, age: Int | Null)

  val persons = spark.createDataSeq(Seq(
    Person("Alice", age = 30),
    Person("Bob", age = 15),
    Person("Charlie", age = 35)
  ))

  // Filter with simple conditions
  val adults = persons.filter(_.age.orElse(0) > 18)
  adults.show()
  // +-------+---+
  // |   name|age|
  // +-------+---+
  // |  Alice| 30|
  // |Charlie| 35|
  // +-------+---+

  // Equality conditions
  val alice = persons.filter(_.name === "Alice")
  alice.show()
  // +-----+---+
  // | name|age|
  // +-----+---+
  // |Alice| 30|
  // +-----+---+
  
  // Inequality conditions
  val notBob = persons.filter(_.name !== "Bob")
  notBob.show()
  // +-------+---+
  // |   name|age|
  // +-------+---+
  // |  Alice| 30|
  // |Charlie| 35|
  // +-------+---+

  // Negation
  val notBob2 = persons.filter(person => !(person.name === "Bob"))
  notBob2.show()
  // +-------+---+
  // |   name|age|
  // +-------+---+
  // |  Alice| 30|
  // |Charlie| 35|
  // +-------+---+

  // Combined logical operators with null handling
  val middleAged = persons.filter(person => nullable(person.age.? > 25 && person.age.? < 35).orElse(false))
  middleAged.show()
  // +-----+---+
  // | name|age|
  // +-----+---+
  // |Alice| 30|
  // +-----+---+
```

[Joining is described in a later section](#joining) but here is an example of how to filter a joined data
using the `departments` and `employees` `DataSeq`s defined in the [Creating a DataSeq](#creating-a-dataseq)
section:
```scala
  // Joining employees and departments
  val joined = employees.join(departments, (emp, dept) => emp.dept_id === dept.id)

  // Filter joined data for only engineering employees
  val engineeringEmployees = joined.filter((emp, dept) => dept.name === "Engineering")
  engineeringEmployees.show()
  // +---+-------+------+---+-----------+
  // | id|   name|deptId| id|       name|
  // +---+-------+------+---+-----------+
  // |  1|  Alice|     1|  1|Engineering|
  // |  3|Charlie|     1|  1|Engineering|
  // +---+-------+------+---+-----------+
```


## Selecting

Wick provides a type-safe way to select and transform columns from your `DataSeq`:
```scala
// Select and transform columns with named tuples
val doubleAges = persons.select(person => (double_age = nullable(person.age.? * 2)))
doubleAges.show()
// +----------+
// |double_age|
// +----------+
// |        60|
// |        30|
// |        70|
// +----------+

// Chain selections for multiple transformations
val quadrupleAges = doubleAges.select(row => (quadruple_age = nullable(row.double_age.? * 2)))
quadrupleAges.show()
// +-------------+
// |quadruple_age|
// +-------------+
// |          120|
// |           60|
// |          140|
// +-------------+
```


## Joining

Wick supports type-safe joins between DataSeqs.
Using the `departments`, `employees` and `titles` `DataSeq`s defined in the [Creating a DataSeq](#creating-a-dataseq)
section:
```scala
  // Join two DataSeqs
  val empDepts = employees.join(departments, _.dept_id === _.id)

  // Select from joined data with type-safe column access
  val selected = empDepts.select((emp, dept) => (
    emp_name = emp.name,
    dept_name = dept.name
  ))
  selected.show()
  // +--------+-----------+
  // |emp_name|  dept_name|
  // +--------+-----------+
  // |   Alice|Engineering|
  // |     Bob|  Marketing|
  // | Charlie|Engineering|
  // +--------+-----------+
```

Multiple joins:
```scala
  // Chain multiple joins
  val empDeptTitle = employees
  .join(departments, (emp, dept) => emp.dept_id === dept.id)
  .join(titles, (emp, dept, title) => emp.title_id === title.id)

  val finalResult = empDeptTitle.select((emp, dept, title) => (
    emp_name = emp.name,
    dept_name = dept.name,
    title_name = title.name,
    managing = title.managing
  ))
  finalResult.show()
  // +--------+-----------+-----------------+--------+
  // |emp_name|  dept_name|       title_name|managing|
  // +--------+-----------+-----------------+--------+
  // |   Alice|Engineering|Software Engineer|   false|
  // |     Bob|  Marketing|       Ad Manager|    true|
  // | Charlie|Engineering|Software Engineer|   false|
  // +--------+-----------+-----------------+--------+
```


## Aggregating

Wick provides type-safe grouping and aggregation operations:

```scala
// `*` is the wildcard passed to `count` (equivalent to SQL's `count(*)`); it is backticked because `*` is reserved
// in Scala for wild imports.
import com.netflix.wick.functions.{count, `*`}

  // Group by a computed column and aggregate
  val aggregated = employees
    .groupBy(emp => (dept_id = emp.dept_id))
    .agg(emp => (population = count(`*`)))
  aggregated.show()
  // +-------+----------+
  // |dept_id|population|
  // +-------+----------+
  // |      1|         2|
  // |      2|         1|
  // +-------+----------+

  // Access both grouping keys and aggregated values
  val summary = aggregated.select(row => (
    dept_id = row.dept_id,
    population = row.population
  ))
```

### Type safety guarantees

Aggregations of grouped data only compiles for Scalar expressions. For example the follow does not compile:
```scala ignore
  persons
    .groupBy(person => (age_group = person.age))
    .agg(person => (age_sum = person.age)) // This does not compile because person.age is not an aggregation
                                          ^
(age_sum : com.netflix.wick.column.Expr[Int | Null]) is not a NamedTuple of ScalarExpr[?].
```

Instead maybe you wanted to do:
```scala
import com.netflix.wick.functions.sum

  persons
    .groupBy(person => (age_group = person.age))
    .agg(person => (age_sum = sum(person.age))) // With the sum, it's better!
```


## Ordering/Sorting

Wick provides type-safe ordering operations that ensure you can only sort by orderable columns at compile time:

```scala
// Sort by a single column (ascending by default)
val byAge = persons.orderBy(_.age)
byAge.show()
// +-------+---+
// |   name|age|
// +-------+---+
// |    Bob| 15|
// |  Alice| 30|
// |Charlie| 35|
// +-------+---+

// Sort by multiple columns 
val byAgeAndName = persons.orderBy(person => (person.age, person.name))
byAgeAndName.show()
// +-------+---+
// |   name|age|
// +-------+---+
// |    Bob| 15|
// |  Alice| 30|
// |Charlie| 35|
// +-------+---+
```

### Explicit sort direction with `asc` and `desc`

For explicit control over sort direction, use the `asc` and `desc` functions:

```scala
import com.netflix.wick.functions.{asc, desc}

// Sort by age descending
val byAgeDesc = persons.orderBy(person => desc(person.age))
byAgeDesc.show()
// +-------+---+
// |   name|age|
// +-------+---+
// |Charlie| 35|
// |  Alice| 30|
// |    Bob| 15|
// +-------+---+

// Sort by multiple columns with mixed directions
val mixedSort = persons.orderBy(person => (desc(person.age), asc(person.name)))
mixedSort.show()
// +-------+---+
// |   name|age|
// +-------+---+
// |Charlie| 35|
// |  Alice| 30|
// |    Bob| 15|
// +-------+---+
```

### Type safety guarantees

Sorting a `DataSeq` only compiles if used on supported types, shielding your job from resulting in a:
```
ExtendedAnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "col ASC NULLS FIRST" due to data type mismatch: The `sortorder` does not support ordering on type "MAP<STRING, INT>".
```

```scala
case class ComplexData(name: String, metadata: Map[String, Int])

val complex = spark.createDataSeq(Seq(
  ComplexData("Alice", Map("score" -> 100)),
  ComplexData("Bob", Map("score" -> 85))
))

// This will NOT compile - Map types are not orderable
// complex.orderBy(_.metadata) // Compilation error!
```


## Column operations

Wick has safer column operations that will make sure the transformations you make are legal at compile time rather
than at runtime, after you packaged and deployed your job.

### Boolean operands

The `||` or `&&` boolean operands are only valid on `Expr[Boolean]` and `Expr[Boolean | Null]`, therefore code like
`person.age || person.name` will simply not compile because it does not make any sense!

### Numeric operands

The usage of `+`, `-`, `*`, `/`, `<`, `<=`, `>` and `>=` numeric operands only compiles if used on supported
numeric columns and their nullable types (like `Option[Int]`).

### Null handling

Handling nulls has never been so easy. Wick strikes a good balance between null safety and ergonomics.
For example, if you need to add 2 nullable integers `Int | Null`, you can use `.?` inside a `nullable`:
```scala
val nullAdditions = persons.select(person => (addition = nullable(person.age.? + person.age.?)))
```
If any of the expressions where `.?` is called is null, it is bubbled up to the result of `nullable`.

Another useful function is `.orElse()`, which is recommended over `coalesce()` since it can track if the expression is
still nullable or not with the `-Yexplicit-nulls` Scala compiler option:
```scala
import com.netflix.wick.column.orElse

val nullFreePerson = persons.select(person => (name = person.name, age_or_zero = person.age.orElse(0)))
```
Given that `person.age` is of type `Int | Null`, `person.age.orElse(0)` changes the type to `Int`, making sure the
resulting `age_or_zero` column does not have nulls.

Now that the `age_or_zero` is not nullable calling `orElse` on this column would not work:
```scala ignore
nullFreePerson.select(person => (name = person.name, age_or_zero = person.age_or_zero.orElse(-1)))
[error]                                                            ^^^^^^^^^^^^^^^^^^^^^^^^^
[error] value orElse is not a member of com.netflix.wick.column.Expr[Int].
```

### `asc`, `desc`, `min` and `max`

Using any of `asc`, `desc`, `min` and `max` operands only compiles if used on supported types, shielding your job from
resulting in a:
```
ExtendedAnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "col ASC NULLS FIRST" due to data type mismatch: The `sortorder` does not support ordering on type "MAP<STRING, INT>".
```


For more comprehensive examples, please check the [test folder](test/src/com/netflix/wick/).


## Related projects

* Wick is heavily inspired from [Going structural with Named Tuples by Jamie Thompson](https://www.youtube.com/watch?v=Qeavi9M65Qw) ([demo repo](https://github.com/bishabosha/scalar-2025))
* [Iskra](https://github.com/VirtusLab/iskra) (Using Macros and not very IDE friendly)
* [Frameless](https://github.com/typelevel/frameless) (Scala 2 and outdated)
* [spark-compiletime](https://github.com/vbergeron/spark-compiletime)
