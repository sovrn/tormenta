/*
 Copyright 2012 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.tormenta.spout

import java.util.{ Map => JMap }
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.task.TopologyContext
import org.apache.storm.tuple.Values
import org.apache.storm.tuple.Fields
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.topology.IRichSpout

trait BaseSpout[+T] extends BaseRichSpout with Spout[T] { self =>

  var collector: SpoutOutputCollector = null

  override def openHook(f: => TopologyContext => Unit) =
    new BaseSpout[T] {
      override def fieldName = self.fieldName
      override def onEmpty = self.onEmpty
      override def poll = self.poll
      override def callOnOpen = (c: TopologyContext) => { f(c); self.callOnOpen(c) }
    }

  def callOnOpen: (TopologyContext) => Unit = { (TopologyContext) => Unit }

  override def open(conf: JMap[String, Object], context: TopologyContext, coll: SpoutOutputCollector) {
    callOnOpen(context)
    collector = coll
  }

  def fieldName: String = "item"

  /**
   * Override to supply new tuples.
   */
  def poll: TraversableOnce[T]

  /**
   * Override this to change the default spout behavior if poll
   * returns an empty list.
   */
  def onEmpty: Unit = Thread.sleep(50)

  override def getSpout: IRichSpout = this

  override def flatMap[U](fn: T => TraversableOnce[U]) =
    new BaseSpout[U] {
      override def fieldName = self.fieldName
      override def onEmpty = self.onEmpty
      override def poll = self.poll.flatMap(fn)
      override def callOnOpen = self.callOnOpen
    }

  override def nextTuple {
    poll match {
      case Nil => onEmpty
      case items => items.foreach { item =>
        collector.emit(new Values(item.asInstanceOf[AnyRef]))
      }
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields(fieldName))
  }
}
