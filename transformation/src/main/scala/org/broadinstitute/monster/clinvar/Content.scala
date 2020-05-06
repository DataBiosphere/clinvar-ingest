package org.broadinstitute.monster.clinvar

import ujson.StringRenderer
import upack.{Arr, Msg, Obj}

import scala.collection.mutable

object Content {

  private val renderer = StringRenderer()

  /**
    * Encode unmodeled data so it can be included in a string column.
    *
    * If the data is empty, it will be dropped.
    */
  def encode(msg: Msg): Option[String] =
    if (msg.obj.isEmpty) {
      None
    } else {
      Some(upack.transform(sortMsg(msg), renderer).toString)
    }

  /**
    * Hacky sort algorithm for upack Msgs.
    *
    * Used in an attempt to keep 'content' outputs consistent across
    * releases.
    */
  private def sortMsg(msg: Msg): Msg = {
    val sorted = mutable.LinkedHashMap.empty[Msg, Msg]
    msg.obj.keysIterator.toList.sortBy(_.str).foreach { k =>
      val original = msg.obj(k)
      val subSorted = original match {
        case o: Obj => sortMsg(o)
        case Arr(buf) =>
          Arr(buf.map(sortMsg).sortBy(upack.transform(_, renderer).toString))
        case _ => original
      }
      sorted.put(k, subSorted)
    }
    Obj(sorted)
  }
}
