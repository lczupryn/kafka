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

import kafka.zookeeper.{AsyncResponse, ExistsRequest, ExistsResponse, ResponseMetadata}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

class ZkExists(client: AtomixClient, request: ExistsRequest,
    callback: AsyncResponse => Unit, responseMetadata: () => ResponseMetadata) extends ZkCommand {
  override def execute(): Option[KeeperException.Code] = {
    val node = client.clusterState.get( request.path )
    if ( node == null ) {
      return Some( Code.NONODE )
    }
    callback(
      ExistsResponse(
        Code.OK, request.path, request.ctx, new AtomixStat( NodeUtils.decode( request.path, node.value() ), node.version() ),
        responseMetadata()
      )
    )
    None
  }

  override def reportUnsuccessful(code: KeeperException.Code): Unit = {
    callback( ExistsResponse( code, request.path, request.ctx, null, responseMetadata() ) )
  }
}
