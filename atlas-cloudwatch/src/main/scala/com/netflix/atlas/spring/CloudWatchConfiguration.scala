/*
 * Copyright 2014-2025 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.spring

import org.apache.pekko.actor.ActorSystem
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.atlas.cloudwatch.AwsAccountSupplier
import com.netflix.atlas.cloudwatch.CloudWatchDebugger
import com.netflix.atlas.cloudwatch.CloudWatchMetricsProcessor
import com.netflix.atlas.cloudwatch.CloudWatchPoller
import com.netflix.atlas.cloudwatch.CloudWatchRules
import com.netflix.atlas.cloudwatch.PublishRouter
import com.netflix.atlas.cloudwatch.RedisClusterCloudWatchMetricsProcessor
import com.netflix.atlas.cloudwatch.NetflixTagger
import com.netflix.atlas.cloudwatch.Tagger
import com.netflix.iep.aws2.AwsClientFactory
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Spectator.globalRegistry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import redis.clients.jedis.Connection
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster

import java.util.Optional

/**
  * Configures the binding for the cloudwatch client and poller.
  */
@Configuration
class CloudWatchConfiguration extends StrictLogging {

  @Bean
  def cloudWatchRules(config: Config): CloudWatchRules = new CloudWatchRules(config)

  @Bean
  def getCloudWatchPoller(
    config: Config,
    registry: Optional[Registry],
    leaderStatus: LeaderStatus,
    rules: CloudWatchRules,
    accounts: AwsAccountSupplier,
    clientFactory: AwsClientFactory,
    processor: CloudWatchMetricsProcessor,
    debugger: CloudWatchDebugger,
    system: ActorSystem
  ): CloudWatchPoller = {
    val r = registry.orElseGet(() => globalRegistry())
    new CloudWatchPoller(
      config,
      r,
      leaderStatus,
      accounts,
      rules,
      clientFactory,
      processor,
      debugger
    )(system)
  }

  @Bean
  def tagger(config: Config): Tagger = new NetflixTagger(
    config.getConfig("atlas.cloudwatch.tagger")
  )

  @Bean
  def publishRouter(
    config: Config,
    registry: Optional[Registry],
    tagger: Tagger,
    httpClient: PekkoHttpClient,
    system: ActorSystem,
    leaderStatus: LeaderStatus
  ): PublishRouter = {
    val r = registry.orElseGet(() => globalRegistry())
    new PublishRouter(config, r, tagger, httpClient, leaderStatus)(system)
  }

  @Bean
  def redisCache(
    config: Config,
    registry: Optional[Registry],
    tagger: Tagger,
    jedis: JedisCluster,
    leaderStatus: LeaderStatus,
    rules: CloudWatchRules,
    publishRouter: PublishRouter,
    debugger: CloudWatchDebugger,
    system: ActorSystem
  ): CloudWatchMetricsProcessor = {
    val r = registry.orElseGet(() => globalRegistry())
    new RedisClusterCloudWatchMetricsProcessor(
      config,
      r,
      tagger,
      jedis,
      leaderStatus,
      rules,
      publishRouter,
      debugger
    )(system)
  }

  @Bean
  def getTagger(config: Config): NetflixTagger = {
    new NetflixTagger(config.getConfig("atlas.cloudwatch.tagger"))
  }

  @Bean
  def httpClient(system: ActorSystem): PekkoHttpClient = {
    PekkoHttpClient.create("PubProxy", system)
  }

  /**
    * Purposely giving each requestee a different cluster client in order to reduce
    * connection pool contention when scraping gauges.
    *
    * @param config
    *   The Typesafe config.
    * @return
    *   A Jedis cluster client.
    */
  @Bean
  def getJedisClient(config: Config): JedisCluster = {
    val poolConfig = new GenericObjectPoolConfig[Connection]()
    poolConfig.setMaxTotal(config.getInt("atlas.cloudwatch.redis.connection.pool.max"))
    val cluster =
      config.getString("iep.leader.valkeycluster.uri") // RedisClusterConfig.getClusterName(config)
    logger.info(s"Using Redis cluster ${cluster}")
    new JedisCluster(
      new HostAndPort(cluster, config.getInt("atlas.cloudwatch.redis.connection.port")),
      config.getInt("atlas.cloudwatch.redis.cmd.timeout"),
      poolConfig
    )
  }

  @Bean
  def debugger(config: Config, registry: Optional[Registry]): CloudWatchDebugger = {
    val r = registry.orElseGet(() => globalRegistry())
    new CloudWatchDebugger(config, r)
  }
}
