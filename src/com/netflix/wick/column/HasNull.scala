package com.netflix.wick.column

/** True if Null is part of type A (i.e. A is nullable). */
type HasNull[A] = Null match
  case A => true
  case _ => false
