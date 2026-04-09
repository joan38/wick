package com.netflix.wick.model

case class Student(name: String, score1: Int, score2: Int, score3: Int)
case class GradeBook(student: String, grades: Seq[Int])
case class Course(name: String, studentGroups: Seq[Seq[String]])
case class NumberMatrix(name: String, matrix: Seq[Seq[Int]])
case class Playlist(name: String, songs: Seq[String])
case class NumberSequence(name: String, numbers: Seq[Int])
