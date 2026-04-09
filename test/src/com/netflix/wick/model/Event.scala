package com.netflix.wick.model

import java.sql.Timestamp

case class Event(id: Int, eventTime: Timestamp, data: String)
