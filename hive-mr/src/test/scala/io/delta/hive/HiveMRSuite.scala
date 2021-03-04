/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.hive

import java.io.{Closeable, File}
import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

class HiveMRSuite extends HiveConnectorTest {

  override val engine: String = "mr"

  override def createCluster(namenode: String, conf: Configuration, tempPath: File): Closeable = {
    val jConf = new JobConf(conf);
    jConf.set("yarn.scheduler.capacity.root.queues", "default");
    jConf.set("yarn.scheduler.capacity.root.default.capacity", "100");
    jConf.setInt(MRJobConfig.MAP_MEMORY_MB, 512);
    jConf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 512);
    jConf.setInt(MRJobConfig.MR_AM_VMEM_MB, 128);
    jConf.setInt(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, 512);
    jConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    jConf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 512);
    jConf.setInt(YarnConfiguration.APPLICATION_HISTORY_ENABLED, 1);
    jConf.set("yarn.log-aggregation-enable", "false");
    jConf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, 1000000000);
    jConf.set("yarn.log-aggregation.TFile.remote-app-log-dir", "file:///tmp/tarek_logs_TF");
    jConf.set("yarn.log-aggregation.file-formats", "TFile");
    jConf.set("yarn.nodemanager.remote-app-log-dir", "file:///tmp/tarek_logs");
    val mr = new MiniMRYarnCluster(namenode, 1)
    mr.init(jConf)
    mr.start()
    new Closeable {
      override def close(): Unit = {
        mr.stop()
      }
    }
  }
}
