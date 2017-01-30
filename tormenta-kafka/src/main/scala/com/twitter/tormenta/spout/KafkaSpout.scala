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

import com.twitter.tormenta.scheme.Scheme
import org.apache.storm.kafka.spout.{ KafkaSpout => StormKafkaSpout, KafkaSpoutConfig, ZkHosts }
import java.util.{HashMap => JHashMap}
import backtype.storm.task.TopologyContext
import com.twitter.tormenta.spout.SchemeSpout
import com.twitter.tormenta.spout.RichStormSpout

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class KafkaSpout[+T](scheme: Scheme[T], bootStrapServers: String, groupId: String, topic: String, keyDeserializer: String, valueDeserializer: String, forceStartOffsetTime: Int = -1)
    extends SchemeSpout[T] {
  override def getSpout[R](transformer: Scheme[T] => Scheme[R], callOnOpen: => TopologyContext => Unit) = {
    // Spout ID needs to be unique per spout, so create that string by taking the topic and appID.
    val spoutId = topic + appID
    val consumerProps = new JHashMap[String,String]
    consumerProps.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, bootStrapServers)
    consumerProps.put(KafkaSpoutConfig.Consumer.GROUP_ID, groupId)
    consumerProps.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, keyDeserializer)
    consumerProps.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, valueDeserializer)
    
    val spoutConfig = new KafkaSpoutConfig.Builder(consumerProps)
    //val spoutConfig = new SpoutConfig(new ZkHosts(zkHost, brokerZkPath), topic, zkRoot, spoutId)

    spoutConfig.scheme = transformer(scheme)
    spoutConfig.forceStartOffsetTime(forceStartOffsetTime)

    new RichStormSpout(new StormKafkaSpout(spoutConfig), callOnOpen)
  }
}
