import com.island.ohara.client.configurator.v0.{BrokerApi, NodeApi, WorkerApi, ZookeeperApi}
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.common.util.{CommonUtils, Releasable}
import com.island.ohara.configurator.Configurator
import org.junit.{After, Test}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class TestConfiguratorClient {
  //Connect to configurator service
  private[this] val configurator = Configurator.builder
    .hostname("192.168.56.103")
    .port(12345)
    .build()

  @Test
  def test(): Unit = {
    val nodes: Seq[Node] = Seq(Node(
      hostname = "192.168.56.103",
      port = Some(22),
      user = Some("ohara"),
      password = Some("oharastream"),
      services = Seq.empty,
      lastModified = System.currentTimeMillis,
      validationReport = None,
      tags = Map.empty
    ))

    // Create Node
    val nodeApi = NodeApi.access.hostname(configurator.hostname).port(configurator.port)
    nodes.foreach { node =>
      result(
        nodeApi.request.hostname(node.hostname).port(node._port).user(node._user).password(node._password).create())
    }

    //Create zookeeper cluster
    val zkApi = ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port)

    //Start zookeeper cluster
    val zkClusterName: String = "zk"
    result(zkApi.request
      .name(zkClusterName)
      .clientPort(CommonUtils.availablePort)
      .electionPort(CommonUtils.availablePort)
      .peerPort(CommonUtils.availablePort)
      .nodeNames(Set(nodes.head.hostname))
      .create())
    result(zkApi.start(zkClusterName))


    val bkApi = BrokerApi.access.hostname(configurator.hostname).port(configurator.port)

    //Create broker cluster
    val bkClusterName: String = "bk"
    result(bkApi.request
      .name(bkClusterName)
      .clientPort(CommonUtils.availablePort)
      .exporterPort(CommonUtils.availablePort)
      .jmxPort(CommonUtils.availablePort)
      .zookeeperClusterName(zkClusterName)
      .nodeNames(Set(nodes.head.hostname))
      .create())

    //Start broker cluster
    result(bkApi.start(bkClusterName))

    val wkApi = WorkerApi.access.hostname(configurator.hostname).port(configurator.port)
    val wkClusterName = "wk"

    //Create worker cluster
    result(wkApi.request
      .name(wkClusterName)
      .clientPort(CommonUtils.availablePort)
      .jmxPort(CommonUtils.availablePort)
      .brokerClusterName(bkClusterName)
      .nodeNames(Set(nodes.head.hostname))
      .create())

    //Start worker cluster
    result(wkApi.start(wkClusterName))
  }

  @After
  def after(): Unit = {
    Releasable.close(configurator)
  }

  def result[T](f: Future[T]): T = Await.result(f, 1 minutes)
}
