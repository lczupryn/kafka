/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.atomix

import java.nio.charset.StandardCharsets

import kafka.common.KafkaException
import kafka.utils.Json

import scala.collection.JavaConverters._
import scala.collection.mutable

private[atomix] object NodeUtils {
  def encode(node: AtomixNode): String = {
    val payload = if (node.getData == null) "[null]" else new String( node.getData, StandardCharsets.UTF_8 )
    val jsonMap = mutable.Map(
      "data" -> payload,
      "version" -> node.getVersion, // Version corresponds to data version from ZooKeeper (node level).
      "owner" -> node.getOwner
    )
    if ( node.isEphemeral ) {
      jsonMap.put( "updatedOn", System.currentTimeMillis() )
    }
    Json.encodeAsString( jsonMap.asJava )
  }

  def decode(path: String, json: String): AtomixNode = {
    if ( json == null ) {
      return null
    }
    Json.tryParseBytes( json.getBytes( StandardCharsets.UTF_8 ) ) match {
      case Right(js) =>
        val node = js.asJsonObject
        val data = node( "data" ).to[String]
        val version = node( "version" ).to[Int]
        val owner = node( "owner" ).to[Long]
        new AtomixNode( if ( data == "[null]" ) null else data.getBytes( StandardCharsets.UTF_8 ), version, owner )
      case Left(e) =>
        throw new KafkaException( s"Failed to parse Atomix node $path, content: $json", e )
    }
  }
}
