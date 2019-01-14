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

import kafka.zk.ZkVersion
import kafka.zookeeper._
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

class ZkDelete(client: AtomixClient, request: DeleteRequest,
    callback: AsyncResponse => Unit, responseMetadata: () => ResponseMetadata) extends ZkCommand {
  override def execute(): Option[KeeperException.Code] = {
    val value = client.clusterState.get( request.path )
    if ( value == null ) {
      return Some( Code.NONODE )
    }
    else {
      val valueNode = NodeUtils.decode( request.path, value.value() )
      if ( ZkVersion.MatchAnyVersion == request.version ) {
        client.clusterState.remove( request.path )
        callback(
          DeleteResponse( Code.OK, request.path, request.ctx, responseMetadata() )
        )
      }
      else {
        val removed = client.clusterState.remove( request.path, value.version() )
        if ( ! removed ) {
          return Some( Code.BADVERSION )
        }
        callback(
          DeleteResponse( Code.OK, request.path, request.ctx, responseMetadata() )
        )
      }
    }
    None
  }

  override def reportUnsuccessful(code: KeeperException.Code): Unit = {
    callback( DeleteResponse( code, request.path, request.ctx, responseMetadata() ) )
  }
}
