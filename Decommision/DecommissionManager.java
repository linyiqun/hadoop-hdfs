/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.CyclicIteration;

/**
 * Manage node decommissioning.
 * 节点Decommission操作状态管理器
 */
class DecommissionManager {
  static final Log LOG = LogFactory.getLog(DecommissionManager.class);
  
  //名字空间系统
  private final FSNamesystem fsnamesystem;

  DecommissionManager(FSNamesystem namesystem) {
    this.fsnamesystem = namesystem;
  }

  /** Periodically check decommission status. */
  //监控方法
  class Monitor implements Runnable {
    /** recheckInterval is how often namenode checks
     *  if a node has finished decommission
     * 定期检查周期
     */
    private final long recheckInterval;
    /** The number of decommission nodes to check for each interval */
    private final int numNodesPerCheck;
    /** firstkey can be initialized to anything. */
    private String firstkey = "";

    Monitor(int recheckIntervalInSecond, int numNodesPerCheck) {
      this.recheckInterval = recheckIntervalInSecond * 1000L;
      this.numNodesPerCheck = numNodesPerCheck;
    }

    /**
     * Check decommission status of numNodesPerCheck nodes
     * for every recheckInterval milliseconds.
     */
    public void run() {
      for(; fsnamesystem.isRunning(); ) {
        synchronized(fsnamesystem) {
          //调用check()方法
          check();
        }
  
        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
          LOG.info("Interrupted " + this.getClass().getSimpleName(), ie);
        }
      }
    }
    
    private void check() {
      int count = 0;
      //遍历每个数据节点
      for(Map.Entry<String, DatanodeDescriptor> entry
          : new CyclicIteration<String, DatanodeDescriptor>(
              fsnamesystem.datanodeMap, firstkey)) {
        final DatanodeDescriptor d = entry.getValue();
        firstkey = entry.getKey();
 
        //如果数据节点正处于decommison操作的话,则做检查
        if (d.isDecommissionInProgress()) {
          try {
            //调用fsnamesystem的checkDecommissionStateInternal方法,此方法内部又会调用isReplicationInProgress进行副本的
            //情况判断
            fsnamesystem.checkDecommissionStateInternal(d);
          } catch(Exception e) {
            LOG.warn("entry=" + entry, e);
          }
          if (++count == numNodesPerCheck) {
            return;
          }
        }
      }
    }
  }
}
