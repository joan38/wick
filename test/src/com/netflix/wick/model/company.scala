package com.netflix.wick.model

case class Department(id: Int, name: String)
case class Employee(id: Int, name: String, dept_id: Int, title_id: Int)
case class Title(id: Int, name: String, managing: Boolean)
