package org.broadinstitute.monster.clinvar

import ujson.StringRenderer
import upack._

import scala.collection.mutable

object Content {

  /**
    * Encode unmodeled data so it can be included in a string column.
    *
    * If the data is empty, it will be dropped. Otherwise, it will be
    * converted to a canonical form, then encoded with no extra whitespace.
    */
  def encode(msg: Msg): Option[String] =
    msg match {
      case Null                        => None
      case Obj(kvs) if kvs.isEmpty     => None
      case Arr(items) if items.isEmpty => None
      case other                       => Some(upack.transform(sort(other), StringRenderer()).toString)
    }

  /**
    * 'Sort' the entries of an arbitrary upack Msg.
    *
    * For arrays, sorting is done by first sorting each item in the array,
    * then comparing the 'value' of each item in the result.
    *
    * For objects, sorting is done by comparing the 'value' of each key in
    * the map; values are sorted as they're added to the output.
    *
    * For scalars, sorting is a no-op.
    */
  private def sort(msg: Msg): Msg =
    msg match {
      case Arr(items) =>
        val sorted = new mutable.ArrayBuffer[Msg]()
        items.foreach(i => sorted += sort(i))
        new Arr(sorted.sortWith(compare))

      case Obj(kvs) =>
        val sorted = new mutable.LinkedHashMap[Msg, Msg]()
        kvs.keys.toList.map(sort).sortWith(compare).foreach(k => sorted.update(k, sort(kvs(k))))
        Obj(sorted)

      case scalar => scalar
    }

  /**
    * Compare two arbitrary upack Msgs.
    *
    * If the two msgs are the same subtype, we can compare their wrapped values.
    * If they are different subtypes, we impose a made-up ordering for consistency.
    */
  private def compare(msg1: Msg, msg2: Msg): Boolean =
    (msg1, msg2) match {
      case (Null, Null)               => false
      case (Bool(b1), Bool(b2))       => b1 < b2
      case (Int32(i1), Int32(i2))     => i1 < i2
      case (Int64(i1), Int64(i2))     => i1 < i2
      case (UInt64(i1), UInt64(i2))   => i1 < i2
      case (Float32(f1), Float32(f2)) => f1 < f2
      case (Float64(f1), Float64(f2)) => f1 < f2
      case (Str(s1), Str(s2))         => s1 < s2
      case (Binary(b1), Binary(b2))   => new String(b1) < new String(b2)
      case (Arr(a1), Arr(a2))         => compareArrs(a1, a2)
      case (Obj(o1), Obj(o2))         => compareObjs(o1, o2)
      case (Ext(t1, _), Ext(t2, _))   => t1 < t2
      case (other1, other2)           => value(other1) < value(other2)
    }

  /**
    * Compare the items of two upack arrays.
    *
    * Bigger arrays are considered to have greater values. If sizes are
    * equal, we compare the elements of pairwise values.
    */
  private def compareArrs(
    a1: mutable.ArrayBuffer[Msg],
    a2: mutable.ArrayBuffer[Msg]
  ): Boolean =
    if (a1.length != a2.length) {
      a1.length < a2.length
    } else {
      a1.zip(a2).exists { case (item1, item2) => compare(item1, item2) }
    }

  /**
    * Compare the entries of two upack objects.
    *
    * Bigger objects are considered to have greater values. If sizes are
    * equal, we compare the first key from each object.
    *
    * Assumes the object entries have been sorted pre-comparison.
    */
  private def compareObjs(
    o1: mutable.LinkedHashMap[Msg, Msg],
    o2: mutable.LinkedHashMap[Msg, Msg]
  ): Boolean =
    if (o1.size != o2.size) {
      o1.size < o2.size
    } else {
      o1.keys.headOption.zip(o2.keys.headOption).exists { case (k1, k2) => compare(k1, k2) }
    }

  /**
    * Get a numeric value for an arbitrary upack Msg.
    *
    * Values are driven entirely by Msg subtype, to impose an arbitrary ordering
    * in cases when it doesn't make sense to compare wrapped values.
    */
  private def value(msg: Msg): Int =
    msg match {
      case Null       => 0
      case _: Bool    => 1
      case _: Int32   => 2
      case _: Int64   => 3
      case _: UInt64  => 4
      case _: Float32 => 5
      case _: Float64 => 6
      case _: Str     => 7
      case _: Binary  => 8
      case _: Arr     => 9
      case _: Obj     => 10
      case _: Ext     => 11
    }
}
