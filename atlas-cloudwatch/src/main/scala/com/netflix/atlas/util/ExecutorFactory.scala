/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.atlas.util

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
  * Simple shim for unit testing polling.
  */
class ExecutorFactory {

  /**
    * Returns a new fixed thread pool.
    * @param numThreads
    *     The number of threads to allocate.
    * @return
    *     An executor service.
    */
  def createFixedPool(numThreads: Int): ExecutorService = Executors.newFixedThreadPool(numThreads)

}