/*
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


package common.utils.cassandra

import java.net.URI
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{TokenAwarePolicy, ConstantReconnectionPolicy, DCAwareRoundRobinPolicy, DowngradingConsistencyRetryPolicy}

import scala.collection.JavaConversions._

case class CassandraConnectionUri(connectionString: String) {

  private val uri = new URI(connectionString)

  private val additionalHosts = Option(uri.getQuery) match {
    case Some(query) => query.split('&').map(_.split('=')).filter(param => param(0) == "host").map(param => param(1)).toSeq
    case None => Seq.empty
  }

  val host = uri.getHost
  val hosts = Seq(uri.getHost) ++ additionalHosts
  val port = uri.getPort
  val keyspace = uri.getPath.substring(1)

}

object Helper {

  def createSessionAndInitKeyspace(uri: CassandraConnectionUri,
                                   defaultConsistencyLevel: ConsistencyLevel = QueryOptions.DEFAULT_CONSISTENCY_LEVEL) = {
    val cluster = new Cluster.Builder().
      addContactPoints(uri.hosts.toArray: _*).
      withPort(uri.port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).
      withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())).
      build

    val session = cluster.connect
    val metadata = cluster.getMetadata
    val clusterName = metadata.getClusterName
    println(s"Connected to cluster: ${clusterName}")
    for ( host <- metadata.getAllHosts) {
      println(s"Datacenter: ${host.getDatacenter}; Host: ${host.getAddress}; Rack: ${host.getRack}")
    }
    session.execute(s"USE ${uri.keyspace}")
    session
  }

}
