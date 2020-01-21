package org.broadinstitute.monster.common

import better.files.File
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.PipelineSpec
import io.circe.Encoder
import io.circe.derivation.deriveEncoder
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.broadinstitute.monster.common.msg.UpackMsgCoder
import upack._

class StorageIOSpec extends PipelineSpec {
  import StorageIOSpec._

  behavior of "StorageIO"

  private implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  private val jsons = List.tabulate(100) { i =>
    s"""{"foo":"bar","baz":[$i,${i + 1}],"qux":{"even?":${i % 2 == 0}}}"""
  }

  private val msgs = List.tabulate[Msg](100) { i =>
    Obj(
      Str("foo") -> Str("bar"),
      Str("baz") -> Arr(
        Int32(i),
        Int32(i + 1)
      ),
      Str("qux") -> Obj(
        Str("even?") -> Bool(i % 2 == 0)
      )
    )
  }

  private val objects = List.tabulate[Example](100) { i =>
    Example(
      foo = "bar",
      baz = Array(i, i + 1),
      qux = Nested(`even?` = i % 2 == 0)
    )
  }

  it should "read messages as JSON-list on exact matches" in {
    File.temporaryDirectory().foreach { tmpDir =>
      val in = (tmpDir / "data.json").write(jsons.mkString("\n"))
      val (_, readMessages) = runWithLocalOutput { sc =>
        StorageIO.readJsonLists(sc, "Test Single", in.pathAsString)
      }
      readMessages should contain allElementsOf msgs
    }
  }

  it should "read messages as JSON-list on glob matches" in {
    File.temporaryDirectory().foreach { tmpDir =>
      jsons.grouped(10).zipWithIndex.foreach {
        case (lines, i) =>
          (tmpDir / s"part$i.json").write(lines.mkString("\n"))
          ()
      }
      val (_, readMessages) = runWithLocalOutput { sc =>
        StorageIO.readJsonLists(sc, "Test Multiple", s"${tmpDir.pathAsString}/*")
      }
      readMessages should contain allElementsOf msgs
    }
  }

  it should "write generic messages as JSON-list" in {
    File.temporaryDirectory().foreach { tmpDir =>
      runWithRealContext(PipelineOptionsFactory.create()) { sc =>
        StorageIO.writeJsonLists(
          sc.parallelize(msgs),
          "Test Write",
          tmpDir.pathAsString
        )
      }
      val written =
        tmpDir.listRecursively().filter(_.name.endsWith(".json")).flatMap(_.lines).toList
      written should contain allElementsOf jsons
    }
  }

  it should "write modeled messages as JSON-list" in {
    File.temporaryDirectory().foreach { tmpDir =>
      runWithRealContext(PipelineOptionsFactory.create()) { sc =>
        StorageIO.writeJsonLists(
          sc.parallelize(objects),
          "Test Write",
          tmpDir.pathAsString
        )
      }
      val written =
        tmpDir.listRecursively().filter(_.name.endsWith(".json")).flatMap(_.lines).toList
      written should contain allElementsOf jsons
    }
  }
}

object StorageIOSpec {
  case class Nested(`even?`: Boolean)
  case class Example(foo: String, baz: Array[Int], qux: Nested)

  implicit val nestedEncoder: Encoder[Nested] = deriveEncoder
  implicit val exampleEncoder: Encoder[Example] = deriveEncoder
}
