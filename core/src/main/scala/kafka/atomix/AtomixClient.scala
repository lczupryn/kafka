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
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantLock

import io.atomix.cluster.{ClusterMembershipEvent, ClusterMembershipEventListener}
import io.atomix.core.Atomix
import io.atomix.core.counter.AtomicCounter
import io.atomix.core.map.{AtomicCounterMap, AtomicMap, AtomicMapEvent, AtomicMapEventListener}
import io.atomix.core.multimap.AtomicMultimap
import io.atomix.primitive.PrimitiveException
import io.atomix.protocols.raft.{MultiRaftProtocol, ReadConsistency}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils.{KafkaScheduler, Logging}
import kafka.zk.KafkaMetastore
import kafka.zookeeper._
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat

import scala.collection.JavaConverters._
import scala.util.{Random, Try}

/**
 * Client of Atomix cluster that guarantees strong consistency of persisted configuration data
 * and discovery of peer brokers. In particular, implementation has to mimic ZooKeeper behaviour,
 * e.g. tree-like data structure and notification of data updates.
 *
 * @param config path to Atomix configuration file
 * @param sessionTimeoutMs session timeout in milliseconds
 * @param connectionTimeoutMs connection timeout in milliseconds
 * @param admin flag indicating connection from administrative CLI
 */
class AtomixClient(time: Time, config: String, sessionTimeoutMs: Int, connectionTimeoutMs: Int, admin: Boolean)
    extends AutoCloseable with KafkaMetastore with Logging with KafkaMetricsGroup {

  @volatile private var atomix: Atomix = _ // Atomix client.
  @volatile private var currentSessionId: Long = _ // Current session identifier that fences zombie nodes.
  @volatile private var running: Boolean = _ // Flag indicating that client shall be still running.
  @volatile private var connected: Boolean = _ // Are we still conteccted to quorum of nodes?
  private val isConnectedOrExpiredLock = new ReentrantLock()
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()

  private val random = new Random()
  private val expiryScheduler = new KafkaScheduler( threads = 1, "atomix-quorum-monitor" )

  // Distributed primitives.
  private[atomix] var idGenerator: AtomicCounter = _ // Counter to generate unique client session identifiers.
  private[atomix] var sequenceNodes: AtomicCounterMap[String] = _ // Map of sequence node path and current counter.
  private[atomix] var clusterState: AtomicMap[String, String] = _ // Main hash map which stores configuration of the cluster.
  private[atomix] var ephemeralCache: AtomicMultimap[String, String] = _ // Cache mapping broker ID to all of its ephemeral nodes.

  // State change handlers.
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]().asScala
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]().asScala
  private val stateChangeHandlers = new ConcurrentHashMap[String, StateChangeHandler]().asScala

  private val clusterListener: ClusterMembershipEventListener = new ClusterMembershipEventListener {
    override def event(event: ClusterMembershipEvent): Unit = {
      logger.debug( s"Received cluster event: $event" )

      if ( ! quorumAvailable() ) {
        logger.error( "Quorum of nodes not available" )
        return
      }

      event.`type`() match {
        case ClusterMembershipEvent.Type.MEMBER_REMOVED =>
          if ( Try( event.subject().id().id().toInt ).isSuccess
              && brokerId() != event.subject().id().id() ) {
            logger.debug( s"Node ${event.subject().id().id()} left the cluster." )
            // Remove ephemeral data from node that left the cluster.
            cleanUpEphemeralData( event.subject().id().id() )
          }
        case _ =>
      }
    }
  }

  private val configurationListener: AtomicMapEventListener[String, String] = new AtomicMapEventListener[String, String] {
    def isDirectChild(parent: String, child: String): Boolean = {
      if ( parent == child || ! child.startsWith( parent ) ) {
        return false
      }
      val remaining = child.substring( parent.length + 1 )
      return ! remaining.contains( "/" )
    }

    override def event(event: AtomicMapEvent[String, String]): Unit = {
      logger.debug( s"Received configuration event: $event" )
      for ( p <- zNodeChildChangeHandlers.keys ) {
        // Analogical to watcher on ZooKeeper GetChildren operation.
        if ( isDirectChild( p, event.key() )
            && ( AtomicMapEvent.Type.REMOVE == event.`type`() || AtomicMapEvent.Type.INSERT == event.`type`() ) ) {
          zNodeChildChangeHandlers.get( p ).foreach( _.handleChildChange() )
        }
        else if ( p == event.key() && AtomicMapEvent.Type.REMOVE == event.`type`() ) {
          zNodeChildChangeHandlers.get( p ).foreach( _.handleChildChange() )
        }
      }
      for ( p <- zNodeChangeHandlers.keys ) {
        // Analogical to watcher on ZooKeeper GetDataRequest and ExistsRequest operations.
        if ( p == event.key() ) {
          event.`type`() match {
            case AtomicMapEvent.Type.INSERT => zNodeChangeHandlers.get( p ).foreach( _.handleCreation() )
            case AtomicMapEvent.Type.UPDATE => zNodeChangeHandlers.get( p ).foreach( _.handleDataChange() )
            case AtomicMapEvent.Type.REMOVE => zNodeChangeHandlers.get( p ).foreach( _.handleDeletion() )
          }
        }
      }
    }
  }

  initialize()

  private def initialize(): Unit = {
    running = true
    atomix = if ( config.isEmpty ) Atomix.builder().build() else Atomix.builder( config ).build()
    logger.debug( "Joining Atomix cluster" )
    try {
      atomix.start().get( connectionTimeoutMs, TimeUnit.MILLISECONDS ) // Blocks until quorum reached.
    }
    catch {
      case e: TimeoutException => throw new ZooKeeperClientTimeoutException( s"Timed out waiting for Atomix quorum" )
    }
    connected = true

    val protocol = MultiRaftProtocol.builder()
      .withReadConsistency( ReadConsistency.LINEARIZABLE )
      .withMinTimeout( Duration.ofMillis( sessionTimeoutMs ) )
      .withMaxTimeout( Duration.ofMillis( sessionTimeoutMs ) )
      .build()

    idGenerator = atomix.atomicCounterBuilder( "member_id" ).withProtocol( protocol ).build()
    retryOnTimeout() { currentSessionId = idGenerator.incrementAndGet() }
    logger.debug( s"Initial Atomix session ID: $sessionId" )

    sequenceNodes = atomix.atomicCounterMapBuilder( "sequence_nodes" ).withProtocol( protocol ).build()
    clusterState = atomix.atomicMapBuilder[String, String]( "system" ).withProtocol( protocol ).build()
    ephemeralCache = atomix.atomicMultimapBuilder[String, String]( "ephemeral_cache" ).withProtocol( protocol ).build()

    expiryScheduler.startup()

    if ( ! admin ) {
      // At this point broker has successfully joined the cluster, which means that majority
      // of nodes are reachable. In case all brokers were killed abnormally before restart, we need to
      // remove all ephemeral nodes, e.g. /brokers/ids/* and /controller.
      cleanUpEphemeralData()

      atomix.getMembershipService.addListener( clusterListener )
      clusterState.addListener( configurationListener )
    }

    expiryScheduler.schedule( "atomix-quorum-monitor", () => {
      if ( ! quorumAvailable() ) {
        if ( connected ) {
          // Node has disconnected from cluster. Suppose network partitioning.
          stateChangeHandlers.values.foreach( callBeforeInitializingSession )
          connected = false
        }
      }
      else {
        if ( ! connected ) {
          // Connectivity re-established.
          afterConnectionRecovered()
          stateChangeHandlers.values.foreach( callAfterInitializingSession )
          connected = true
          inLock( isConnectedOrExpiredLock ) {
            isConnectedOrExpiredCondition.signalAll()
          }
        }
      }
    }, period = sessionTimeoutMs )
  }

  def sessionId: Long = currentSessionId

  def send[Req <: AsyncRequest](request: Req)(processResponse: Req#Response => Unit): Unit = {
    def callback(response: AsyncResponse): Unit = processResponse(response.asInstanceOf[Req#Response])

    val sendTimeMs = time.hiResClockMs()
    def responseMetadata(): ResponseMetadata = ResponseMetadata(sendTimeMs, receivedTimeMs = time.hiResClockMs())

    var command: ZkCommand = null

    request match {
      case ExistsRequest(path, ctx) =>
        command = new ZkExists( this, request.asInstanceOf[ExistsRequest], callback, responseMetadata )
      case GetDataRequest(path, ctx) =>
        command = new ZkGetData( this, request.asInstanceOf[GetDataRequest], callback, responseMetadata )
      case GetChildrenRequest(path, ctx) =>
        command = new ZkGetChildren( this, request.asInstanceOf[GetChildrenRequest], callback, responseMetadata )
      case CreateRequest(path, data, acl, createMode, ctx) =>
        command = new ZkCreate( this, request.asInstanceOf[CreateRequest], callback, responseMetadata )
      case SetDataRequest(path, data, version, ctx) =>
        command = new ZkSetData( this, request.asInstanceOf[SetDataRequest], callback, responseMetadata )
      case DeleteRequest(path, version, ctx) =>
        command = new ZkDelete( this, request.asInstanceOf[DeleteRequest], callback, responseMetadata )
      case _ => logger.error( s"Unsupported ZooKeeper request: $request" )
    }

    if ( command != null ) {
      try {
        val errorCode = command.execute()
        if ( errorCode.isDefined ) {
          command.reportUnsuccessful( errorCode.get )
        }
      }
      catch {
        case _ @ ( _: PrimitiveException.ClosedSession | _: PrimitiveException.UnknownSession ) =>
          if ( running ) {
            command.reportUnsuccessful( Code.CONNECTIONLOSS )
          }
        case _: PrimitiveException.Timeout =>
          // Atomix reports operation timeout even if quorum of nodes cannot be reached. Simulate ZooKeeper
          // connectivity issue until quorum can be reached again.
          command.reportUnsuccessful( if ( quorumAvailable() ) Code.OPERATIONTIMEOUT else Code.CONNECTIONLOSS )
      }
    }
  }

  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zNodeChangeHandlers.put( zNodeChangeHandler.path, zNodeChangeHandler )
  }

  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.remove( path )
  }

  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    zNodeChildChangeHandlers.put( zNodeChildChangeHandler.path, zNodeChildChangeHandler )
  }

  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    zNodeChildChangeHandlers.remove( path )
  }

  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = {
    if ( stateChangeHandler != null ) {
      stateChangeHandlers.put(stateChangeHandler.name, stateChangeHandler)
    }
  }

  def unregisterStateChangeHandler(name: String): Unit = {
    stateChangeHandlers.remove( name )
  }

  private def callBeforeInitializingSession(handler: StateChangeHandler): Unit = {
    try {
      handler.beforeInitializingSession()
    }
    catch {
      case t: Throwable =>
        error( s"Uncaught error in handler ${handler.name}", t )
    }
  }

  private def callAfterInitializingSession(handler: StateChangeHandler): Unit = {
    try {
      handler.afterInitializingSession()
    }
    catch {
      case t: Throwable =>
        error( s"Uncaught error in handler ${handler.name}", t )
    }
  }

  override def close(): Unit = {
    running = false

    expiryScheduler.shutdown()

    zNodeChangeHandlers.clear()
    zNodeChildChangeHandlers.clear()
    stateChangeHandlers.clear()

    clusterState.close()
    ephemeralCache.close()
    idGenerator.close()
    sequenceNodes.close()

    atomix.stop()
    connected = false
  }

  def retryOnTimeout[A]()(code: => A): A = {
    var result: Option[A] = None
    var attempt: Int = 0
    while ( result.isEmpty ) {
      try {
        attempt += 1
        result = Some( code )
      }
      catch {
        case e: PrimitiveException.Timeout =>
          logger.warn( s"Retrying timed-out call for $attempt time: ${e.getMessage}" )
          // Randomize retry delay to minimize possibility of two nodes competing for same resource concurrently.
          Thread.sleep( random.nextInt( 1000 ) + 1 )
      }
    }
    result.get
  }

  override def handleRequest[Req <: AsyncRequest](request: Req): Req#Response = {
    handleRequests(Seq(request)).head
  }

  override def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    if ( requests.isEmpty ) {
      Seq.empty
    }
    else {
      val responseQueue = new ArrayBlockingQueue[Req#Response](requests.size)
      requests.foreach { request =>
        send(request) { response => responseQueue.add( response ) }
      }
      responseQueue.asScala.toBuffer
    }
  }

  override def waitUntilConnected(): Unit = inLock(isConnectedOrExpiredLock) {
    logger.info( "Waiting for Atomix quorum" )

    var nanos = TimeUnit.MILLISECONDS.toNanos( connectionTimeoutMs )
    while ( ! connected ) {
      if ( nanos <= 0 ) {
        throw new ZooKeeperClientTimeoutException( s"Timed out waiting for Atomix quorum" )
      }
      nanos = isConnectedOrExpiredCondition.awaitNanos( nanos )
    }

    logger.info( "Atomix quorum reachable" )
  }

  private def afterConnectionRecovered(): Unit = {
    retryOnTimeout() {
      // Create unique session identifier and epoch so that we know we have lost network
      // connectivity for some time.
      currentSessionId = idGenerator.incrementAndGet()
      logger.debug( s"New Atomix session ID: $sessionId" )
    }
    if ( ! admin ) {
      cleanUpEphemeralData()
    }
  }

  private def cleanUpEphemeralData(brokerId: String): Unit = {
    val nodeEphemeralData = ephemeralCache.get( brokerId ).value()
    for ( entry <- nodeEphemeralData.asScala ) {
      clusterState.remove( entry )
    }
    ephemeralCache.removeAll( brokerId, nodeEphemeralData )
  }

  private def cleanUpEphemeralData(): Unit = {
    val reachableBrokers = atomix.getMembershipService.getReachableMembers.asScala
      .filter( m => Try( m.id().id().toInt ).isSuccess )
      .map( m => m.id().id() )
    // Remove data from unreachable brokers.
    val persistedBrokers = ephemeralCache.keySet().asScala
    persistedBrokers.foreach(
      b => if ( ! reachableBrokers.contains( b ) ) cleanUpEphemeralData( b )
    )
    // Remove my own data if stale.
    for ( path <- ephemeralCache.get( brokerId() ).value().asScala ) {
      val node = clusterState.get( path )
      if ( node != null && NodeUtils.decode( path, node.value() ).getOwner < currentSessionId ) {
        clusterState.remove( path, node.version() )
      }
    }
  }

  private def quorumAvailable(): Boolean = {
    // Take total number of nodes from configuration of system partition.
    val total = atomix.getPartitionService.getSystemPartitionGroup.getPartitions.asScala.head.members().size()
    // Assume that Atomix member ID equals to Kafka broker identifier.
    val reachable = atomix.getMembershipService.getReachableMembers.asScala.count( m => Try( m.id().id().toInt ).isSuccess )
    reachable.doubleValue() > ( total.doubleValue() / 2.0d )
  }

  private[atomix] def brokerId(): String = atomix.getMembershipService.getLocalMember.id().id()
}

/**
 * Kafka uses only several properties of ZooKeeper statistics, which we need to support.
 */
class AtomixStat(node: AtomixNode, version: Long) extends Stat {
  initialize()
  def initialize(): Unit = {
    setCzxid( version )
    setVersion( node.getVersion )
    setEphemeralOwner( node.getOwner )
    setDataLength( if ( node.getData != null ) node.getData.length else 0 )
  }
  override def equals(other: Any): Boolean = super.equals( other )
  override def hashCode(): Int = super.hashCode()
}

/**
 * Object whose JSON representation is persisted in Atomix distributed map. Atomix does not store
 * owner of ephemeral data and local (per key) version number.
 * <p/>
 * Sample value present in distributed map:
 * {{{
 * {
 *   "data": "{
 *     "listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},
 *     "endpoints":["PLAINTEXT://kafka1:9092"],
 *     ...
 *   }",
 *   "version":0,
 *   "owner":4
 * }
 * }}}
 *
 * @param data Data in JSON format.
 * @param version Key-level version.
 * @param owner Identifier of session that owns ephemeral entry, ''0'' otherwise.
 */
class AtomixNode(data: Array[Byte], version: Int, var owner: Long) {
  def getData: Array[Byte] = data
  def getVersion: Int = version
  def getOwner: Long = owner
  def setOwner(id: Long): Unit = owner = id
  def isEphemeral: Boolean = owner != 0
}
