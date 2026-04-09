package com.netflix.wick
package functions

import org.apache.spark
import org.apache.spark.sql.Column

extension [T](array: Expr[Seq[T]])
  /** Shuffles the elements in the array randomly.
    *
    * This function returns a new array with the same elements as the input array, but in a random order. Each execution
    * may produce a different result due to the random nature of shuffling.
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the shuffled array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Deck(suit: String, cards: Seq[Int])
    * val deck = spark.createDataSeq(Seq(
    *   Deck("hearts", Seq(1, 2, 3, 4, 5))
    * ))
    *
    * // Shuffle the cards randomly
    * val shuffledDeck = deck.select(row => (shuffled_cards = row.cards.shuffle))
    * // Result: random order like [3, 1, 5, 2, 4] (varies each time)
    *   }}}
    */
  def shuffle: LinearExpr[Seq[T]] = LinearExpr(spark.sql.functions.shuffle(new Column(array.underlying)).expr)

extension [T](array: ScalarExpr[Seq[T]])
  /** Shuffles the elements in the array randomly.
    *
    * This function returns a new array with the same elements as the input array, but in a random order. Each execution
    * may produce a different result due to the random nature of shuffling.
    *
    * @return
    *   a LinearExpr[Seq[T]] representing the shuffled array
    *
    * @example
    *   {{{
    * import com.netflix.wick.{*, given}
    *
    * case class Deck(suit: String, cards: Seq[Int])
    * val deck = spark.createDataSeq(Seq(
    *   Deck("hearts", Seq(1, 2, 3, 4, 5))
    * ))
    *
    * // Shuffle the cards randomly
    * val shuffledDeck = deck.select(row => (shuffled_cards = row.cards.shuffle))
    * // Result: random order like [3, 1, 5, 2, 4] (varies each time)
    *   }}}
    */
  def shuffle: ScalarExpr[Seq[T]] = ScalarExpr(spark.sql.functions.shuffle(new Column(array.underlying)).expr)
