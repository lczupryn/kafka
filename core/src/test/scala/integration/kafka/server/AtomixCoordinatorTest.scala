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

package integration.kafka.server

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{CoreUtils, Logging, TestUtils}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.test.IntegrationTest
import org.junit.Assert._
import org.junit.experimental.categories.Category
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

@Category(Array(classOf[IntegrationTest]))
class AtomixCoordinatorTest extends JUnitSuite with Logging {
  private val atomixStorageDir = System.getProperty( "java.io.tmpdir" ) + "/atomix"
  private val topicName = "test"

  var zkClient: KafkaZkClient = _
  var adminZkClient: AdminZkClient = _
  var broker: KafkaServer = _
  var brokerList: String = _
  val securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  val listenerName: ListenerName = ListenerName.forSecurityProtocol( securityProtocol )

  @Before
  def setUp(): Unit = {
    if ( Files.exists( Paths.get( atomixStorageDir ) ) ) {
      Files.walkFileTree( Paths.get( atomixStorageDir ), new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete( file )
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete( dir )
          FileVisitResult.CONTINUE
        }
      } )
    }
    System.setProperty( "atomix.storage.dir", atomixStorageDir )
    val configs = TestUtils.createBrokerConfigs( 1, "atomix://" + getClass.getResource( "/atomix-server.conf" ).getPath )
    broker = TestUtils.createServer( KafkaConfig.fromProps( configs.head ) )
    brokerList = TestUtils.bootstrapServers( Seq( broker ), listenerName )

    zkClient = KafkaZkClient(
      "atomix://" + getClass.getResource( "/atomix-client.conf" ).getPath, JaasUtils.isZkSecurityEnabled,
      5000, 5000, 100, Time.SYSTEM, admin = true
    )
    adminZkClient = new AdminZkClient( zkClient )
  }

  @After
  def tearDown(): Unit = {
    if ( broker != null ) {
      TestUtils.shutdownServers( Seq( broker ) )
    }
    if ( zkClient != null ) {
      CoreUtils.swallow( zkClient.close(), this )
    }
  }

  @Test
  def testCreateTopicAndSendReceiveData(): Unit = {
    adminZkClient.createTopic( topicName, 2, 1 )
    assertTrue( adminZkClient.getAllTopicConfigs().contains( topicName ) )

    val recordsProduced = Seq(
      new ProducerRecord[Array[Byte], Array[Byte]]( topicName, new Integer(0), "key1".getBytes, "value1".getBytes ),
      new ProducerRecord[Array[Byte], Array[Byte]]( topicName, new Integer(1), "key2".getBytes, "value2".getBytes )
    )

    val producer = TestUtils.createNewProducer( brokerList, securityProtocol = securityProtocol )
    recordsProduced.foreach( producer.send )
    producer.close()

    val consumer = TestUtils.createNewConsumer(
      TestUtils.getBrokerListStrFromServers( Seq( broker ) ), securityProtocol = securityProtocol
    )
    consumer.subscribe( List( topicName ).asJava )
    val recordsReceived = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]()
    TestUtils.waitUntilTrue( () => {
      recordsReceived ++= consumer.poll( 50 ).asScala
      recordsReceived.size == recordsProduced.size
    }, s"Consumed ${recordsReceived.size} records until timeout, but expected $recordsProduced records." )
    consumer.close()

    adminZkClient.deleteTopic( topicName )

    assertEquals( recordsProduced.size, recordsReceived.size )
  }
}
