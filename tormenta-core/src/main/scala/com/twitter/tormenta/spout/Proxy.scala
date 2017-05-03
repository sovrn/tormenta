package com.twitter.tormenta.spout

import com.twitter.tormenta.Externalizer
import java.io.Serializable
import java.util.{ Map => JMap }
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.topology.IRichSpout
import org.apache.storm.task.TopologyContext

/**
 * *
 * Proxied trait for type T
 * allows for overriding certain methods but forwarding behavior of all other methods of T.
 * See com.twitter.storehaus.Proxy for a detailed example.
 */
trait Proxied[T] {
  protected def self: T
}

trait SpoutProxy extends IRichSpout with Proxied[IRichSpout] with Serializable {
  override def open(conf: JMap[String, Object], topologyContext: TopologyContext, outputCollector: SpoutOutputCollector) =
    self.open(conf, topologyContext, outputCollector)
  override def nextTuple = self.nextTuple
  override def declareOutputFields(declarer: OutputFieldsDeclarer) = self.declareOutputFields(declarer)
  override def close = self.close
  override def ack(msgId: Object) = self.ack(msgId)
  override def fail(msgId: Object) = self.fail(msgId)
  override def deactivate = self.deactivate
  override def getComponentConfiguration = self.getComponentConfiguration
  override def activate = self.activate
}

class RichStormSpout(val self: IRichSpout,
    @transient callOnOpen: (TopologyContext) => Unit) extends SpoutProxy {
  val lockedFn = Externalizer(callOnOpen)

  override def open(conf: JMap[String, Object], context: TopologyContext, coll: SpoutOutputCollector) {
    lockedFn.get(context)
    self.open(conf, context, coll)
  }
}
