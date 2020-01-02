/*
 * Copyright 2014-2020 Netflix, Inc.
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
package com.netflix.atlas

import _root_.akka.http.scaladsl.model.HttpRequest
import _root_.akka.http.scaladsl.model.HttpResponse
import _root_.akka.stream.scaladsl.Flow
import _root_.akka.NotUsed
import com.netflix.atlas.akka.AccessLogger

import scala.util.Try

package object druid {

  type HttpClient = Flow[(HttpRequest, AccessLogger), (Try[HttpResponse], AccessLogger), NotUsed]
}
