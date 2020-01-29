package org.broadinstitute.monster.common.msg

import java.io.{InputStream, OutputStream}

import com.google.common.io.ByteStreams
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.util.VarInt
import upack.Msg

/**
  * Beam coder (serde) for uPack messages.
  *
  * Delegates to uPack's built-in binary encoding and parsers.
  */
class UpackMsgCoder extends Coder[Msg] {

  override def encode(value: Msg, outStream: OutputStream): Unit =
    Option(value).foreach { msg =>
      val bytes = upack.write(msg)
      VarInt.encode(bytes.length, outStream)
      outStream.write(bytes)
    }

  override def decode(inStream: InputStream): Msg = {
    val numBytes = VarInt.decodeInt(inStream)
    val bytes = new Array[Byte](numBytes)
    ByteStreams.readFully(inStream, bytes)
    upack.read(bytes)
  }

  override def getCoderArguments: java.util.List[_ <: Coder[_]] =
    java.util.Collections.emptyList()

  override def verifyDeterministic(): Unit =
    throw new Coder.NonDeterministicException(
      this,
      "Equal Msgs don't necessarily encode to the same bytes"
    )
}
