package org.apache.parquet.filter2.udp

import java.lang.{Integer => JInt}

import org.apache.parquet.filter2.predicate.{Statistics, UserDefinedPredicate}

import scala.util.Random

class SampleIntColumn(threshold: Double) extends UserDefinedPredicate[JInt] with Serializable {
  lazy val random = { new Random() }
  val myThreshold = threshold
  override def keep(value: JInt): Boolean = {
    random.nextFloat() < myThreshold
  }

  override def canDrop(statistics: Statistics[JInt]): Boolean = false

  override def inverseCanDrop(statistics: Statistics[JInt]): Boolean = false

  override def toString: String = {
    "%s(%f)".format(getClass.getName, myThreshold)
  }
}
