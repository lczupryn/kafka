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
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.JavaConverters._

class ZkGetChildren(client: AtomixClient, request: GetChildrenRequest,
    callback: AsyncResponse => Unit, responseMetadata: () => ResponseMetadata) extends ZkCommand {
  override def execute(): Option[KeeperException.Code] = {
    val parent = client.clusterState.get( request.path )
    if ( parent == null ) {
      return Some( Code.NONODE )
    }
    // TODO: Will this perform if we have a lot topic-partitions?
    val children = client.clusterState.keySet().asScala
        .filter( k => isDirectChild( request.path, k ) )
        .map( k => k.substring( request.path.length + 1 ) ).toSeq
    callback(
      GetChildrenResponse(
        Code.OK, request.path, request.ctx, children,
        new AtomixStat( NodeUtils.decode( request.path, parent.value() ), parent.version() ), responseMetadata()
      )
    )
    None
  }

  // We are looking only for children, not all descendants.
  private def isDirectChild(parent: String, child: String): Boolean = {
    if ( parent == child || ! child.startsWith( parent ) ) {
      return false
    }
    val remaining = child.substring( parent.length + 1 )
    return ! remaining.contains( "/" )
  }

  override def reportUnsuccessful(code: KeeperException.Code): Unit = {
    callback( GetChildrenResponse( code, request.path, request.ctx, null, null, responseMetadata() ) )
  }
}
