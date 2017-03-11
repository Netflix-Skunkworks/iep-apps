/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.iep.archaius

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import javax.inject.Inject
import javax.inject.Singleton

import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

/**
  * Context for accessing property values from the storage system. This class can be
  * asynchronously updated so that local access to properties does not need to result
  * into a separate call to the storage layer.
  */
@Singleton
class PropertiesContext @Inject() (registry: Registry) extends StrictLogging {

  private val clock = registry.clock()

  /**
    * Tracks the age for the properties cache. This can be used for a simple alert to
    * detect staleness.
    */
  private val lastUpdateTime = registry.gauge(
    "iep.props.cacheAge",
    new AtomicLong(clock.wallTime()),
    Functions.AGE)

  private val updateLatch = new AtomicReference[CountDownLatch](new CountDownLatch(1))
  private val ref = new AtomicReference[PropList]()

  /** Update the properties cache for this context. */
  def update(props: PropList): Unit = {
    lastUpdateTime.set(clock.wallTime())
    ref.set(props)
    logger.debug(s"properties updated from dynamodb, size = ${props.size}")
    updateLatch.get().countDown()
  }

  /**
    * Returns true if properties have been updated at least once. Users of this class should
    * check that it has been properly initialized before consuming properties.
    */
  def initialized: Boolean = ref.get != null

  /** Return all properties. */
  def getAll: PropList = ref.get

  /** Return all properties for the specified cluster. */
  def getClusterProps(cluster: String): PropList = ref.get.filter(_.cluster == cluster)

  /** Used for testing. This returns a latch that will get updated along with properties. */
  private[archaius] def latch: CountDownLatch = {
    val latch = new CountDownLatch(1)
    updateLatch.set(latch)
    latch
  }
}
