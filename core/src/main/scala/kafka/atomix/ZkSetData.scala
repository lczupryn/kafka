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

import java.time.Duration

import kafka.zk.ZkVersion
import kafka.zookeeper._
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.KeeperException

class ZkSetData(client: AtomixClient, request: SetDataRequest,
    callback: AsyncResponse => Unit, responseMetadata: () => ResponseMetadata) extends ZkCommand {
  override def execute(): Option[KeeperException.Code] = {
    val previous = client.clusterState.get( request.path )
    if ( previous == null ) {
      // Node did not exist.
      return Some( Code.NONODE )
    }
    else {
      val previousNode = NodeUtils.decode( request.path, previous.value() )
      val nextVersion = previousNode.getVersion + 1
      val nextNode = new AtomixNode(
        request.data, nextVersion, if ( previousNode.isEphemeral ) client.sessionId else 0
      )
      val next = NodeUtils.encode( nextNode )
      if ( request.version == ZkVersion.MatchAnyVersion ) {
        // We just blindly update the latest version.
        val refreshed = client.clusterState.putAndGet(
          request.path, next, if ( nextNode.isEphemeral ) Duration.ofMillis( client.entryTtl() ) else Duration.ZERO
        )
        if ( nextNode.isEphemeral ) {
          client.ephemeralCache.put( request.path, NodeUtils.decode( request.path, refreshed.value() ) )
        }
        callback(
          SetDataResponse( Code.OK, request.path, request.ctx, new AtomixStat( nextNode, refreshed.version() ), responseMetadata() )
        )
      }
      else if ( previousNode.getVersion != request.version ) {
        // Other broker updated the version in the meantime.
        return Some( Code.BADVERSION )
      }
      else {
        // TODO: Replace function does not support TTL, but luckily Kafka never updates ephemeral data.
        val updated = client.clusterState.replace( request.path, previous.version(), next )
        val refreshed = client.clusterState.get( request.path )
        if ( updated && NodeUtils.decode( request.path, refreshed.value() ).getVersion == nextVersion ) {
          // We have updated the node and own the latest modification.
          if ( nextNode.isEphemeral ) {
            client.ephemeralCache.put( request.path, NodeUtils.decode( request.path, refreshed.value() ) )
          }
          callback(
            SetDataResponse( Code.OK, request.path, request.ctx, new AtomixStat( nextNode, refreshed.version() ), responseMetadata() )
          )
        }
        else {
          return Some( Code.BADVERSION )
        }
      }
    }
    None
  }

  override def reportUnsuccessful(code: KeeperException.Code): Unit = {
      callback( SetDataResponse( code, request.path, request.ctx, null, responseMetadata() ) )
  }
}
