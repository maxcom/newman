/**
 * Copyright 2012-2013 StackMob
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stackmob

import scalaz._
import scalaz.NonEmptyList._
import scala.concurrent._
import Scalaz._
import java.nio.charset.Charset
import java.net.URL

package object newman extends NewmanPrivate {
  type FutureValidation[Fail, Success] = Future[Validation[Fail, Success]]

  type Header = (String, String)
  type HeaderList = NonEmptyList[Header]
  type Headers = Option[HeaderList]

  object HeaderList {
    implicit val HeaderListShow = new Show[HeaderList] {
      override def shows(headerList: HeaderList): String = {
        headerList.list.map { header =>
          s"${header._1}=${header._2}"
        }.mkString("&")
      }
    }
  }

  object Headers {
    implicit val HeadersEqual = new Equal[Headers] {
      override def equal(headers1: Headers, headers2: Headers): Boolean = (headers1, headers2) match {
        case (Some(h1), Some(h2)) => h1.list === h2.list
        case (None, None) => true
        case _ => false
      }
    }

    implicit val HeadersMonoid: Monoid[Headers] =
      Monoid.instance((mbH1, mbH2) => (mbH1 tuple mbH2).map(h => h._1.append(h._2)), Headers.empty)

    implicit val HeadersShow = new Show[Headers] {
      import HeaderList.HeaderListShow
      override def shows(h: Headers): String = {
        val s = ~h.map { headerList: HeaderList =>
          headerList.shows
        }
        s
      }
    }

    def apply(h: Header): Headers = Headers(nels(h))
    def apply(h: Header, tail: Header*): Headers = Headers(nel(h, tail.toList))
    def apply(h: HeaderList): Headers = h.some
    def apply(h: List[Header]): Headers = h.toNel
    def empty: Option[HeaderList] = Option.empty[HeaderList]
  }

  type RawBody = Array[Byte]
  implicit val RawBodyMonoid: Monoid[RawBody] = Monoid.instance(_ ++ _, Array[Byte]())
  object RawBody {
    def apply(s: String, charset: Charset = Constants.UTF8Charset): Array[Byte] = s.getBytes(charset)
    def apply(b: Array[Byte]): Array[Byte] = b
    lazy val empty = Array[Byte]()
  }

  implicit class RichURL(url: URL) {
    def hostAndPort: (String, Int) = {
      val host = url.getHost
      val port = url.getPort match {
        case -1 => 80
        case other => other
      }
      host -> port
    }
  }
}
