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

import kafka.zookeeper._
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.{CreateMode, KeeperException}

class ZkCreate(client: AtomixClient, request: CreateRequest,
    callback: AsyncResponse => Unit, responseMetadata: () => ResponseMetadata) extends ZkCommand {
  override def execute(): Option[KeeperException.Code] = {
    // Check whether the parent node exists. Assume root is always present.
    val parentPath = request.path.substring( 0, request.path.lastIndexOf( "/" ) )
    if ( parentPath != "" && client.clusterState.get( parentPath ) == null ) {
      return Some( Code.NONODE )
    }
    val node = new AtomixNode(
      request.data, 0, if ( CreateMode.EPHEMERAL == request.createMode ) client.sessionId else 0
    )
    val nodeNode = NodeUtils.encode( node )
    var created = false
    var suffix = ""
    request.createMode match {
      case CreateMode.EPHEMERAL =>
        client.ephemeralCache.put( client.brokerId(), request.path )
        created = client.clusterState.putIfAbsent( request.path, nodeNode ) == null
      case CreateMode.PERSISTENT_SEQUENTIAL =>
        // ZooKeeper represents sequence numbers as 4 bytes signed integer.
        // Counter overflows when incremented above 2147483647.
        suffix = client.sequenceNodes.incrementAndGet( request.path ).toString
        if ( suffix.toLong > 2000000000 ) {
          var previous = suffix.toLong
          var candidate = 1
          while ( ! client.sequenceNodes.replace( request.path, previous, candidate ) ) {
            previous = candidate
            candidate += 1
          }
          suffix = candidate.toString
        }
        suffix = suffix.reverse.padTo( 10, '0' ).reverse
        created = client.clusterState.putIfAbsent( request.path + suffix, nodeNode ) == null
      case CreateMode.PERSISTENT =>
        created = client.clusterState.putIfAbsent( request.path, nodeNode ) == null
    }
    if ( ! created ) {
      return Some( Code.NODEEXISTS )
    }
    callback(
      CreateResponse( Code.OK, request.path, request.ctx, request.path + suffix, responseMetadata() )
    )
    None
  }

  override def reportUnsuccessful(code: KeeperException.Code): Unit = {
    callback( CreateResponse( code, request.path, request.ctx, null, responseMetadata() ) )
  }
}
