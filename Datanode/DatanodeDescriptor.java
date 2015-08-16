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

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableUtils;

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * This data structure is a data structure that is internal
 * to the namenode. It is *not* sent over-the-wire to the Client
 * or the Datnodes. Neither is it stored persistently in the
 * fsImage.
 DatanodeDescriptor数据节点描述类跟踪描述了一个数据节点的状态信息
 **************************************************/
public class DatanodeDescriptor extends DatanodeInfo {
  
  // Stores status of decommissioning.
  // If node is not decommissioning, do not use this object for anything.
  //下面这个对象只与decomission撤销工作相关
  DecommissioningStatus decommissioningStatus = new DecommissioningStatus();

  /** Block and targets pair */
  //数据块以及目标数据节点列表映射类
  public static class BlockTargetPair {
  	//目标数据块
    public final Block block;
    //该数据块的目标数据节点
    public final DatanodeDescriptor[] targets;    

    BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /** A BlockTargetPair queue. */
  //block块目标数据节点类队列
  private static class BlockQueue {
  	//此类维护了BlockTargetPair列表对象
    private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();

    /** Size of the queue */
    synchronized int size() {return blockq.size();}

    /** Enqueue */
    synchronized boolean offer(Block block, DatanodeDescriptor[] targets) { 
      return blockq.offer(new BlockTargetPair(block, targets));
    }

    /** Dequeue */
    synchronized List<BlockTargetPair> poll(int numBlocks) {
      if (numBlocks <= 0 || blockq.isEmpty()) {
        return null;
      }

      List<BlockTargetPair> results = new ArrayList<BlockTargetPair>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }
  }
  
  //临时变量，在后面的方法中会用到
  private volatile BlockInfo blockList = null;
  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  //节点是否活着，通过心跳来判断
  protected boolean isAlive = false;
  protected boolean needKeyUpdate = false;

  // A system administrator can tune the balancer bandwidth parameter
  // (dfs.balance.bandwidthPerSec) dynamically by calling
  // "dfsadmin -setBalanacerBandwidth <newbandwidth>", at which point the
  // following 'bandwidth' variable gets updated with the new value for each
  // node. Once the heartbeat command is issued to update the value on the
  // specified datanode, this value will be set back to 0.
  //带宽属性，可以通过dfsadmin -setBalanacerBandwidth <newbandwidth>这个命令进行动态设置
  //但是一旦途中心跳命令出错，带宽将会被设为0
  private long bandwidth;

  /** A queue of blocks to be replicated by this datanode */
  //此数据节点上待复制的block块列表
  private BlockQueue replicateBlocks = new BlockQueue();
  /** A queue of blocks to be recovered by this datanode */
  //此数据节点上待租约恢复的块列表
  private BlockQueue recoverBlocks = new BlockQueue();
  /** A set of blocks to be invalidated by this datanode */
  //此数据节点上无效待删除的块列表
  private Set<Block> invalidateBlocks = new TreeSet<Block>();

  /* Variables for maintaning number of blocks scheduled to be written to
   * this datanode. This count is approximate and might be slightly higger
   * in case of errors (e.g. datanode does not report if an error occurs 
   * while writing the block).
   */
  //写入这个数据节点的块的数目统计变量
  private int currApproxBlocksScheduled = 0;
  private int prevApproxBlocksScheduled = 0;
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min
  
  // Set to false after processing first block report
  private boolean firstBlockReport = true; 
  
  /** Default constructor */
  public DatanodeDescriptor() {}
  
  /** DatanodeDescriptor constructor
   * @param nodeID id of the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
    this(nodeID, 0L, 0L, 0L, 0);
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation) {
    this(nodeID, networkLocation, null);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param hostName it could be different from host specified for DatanodeID
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation,
                            String hostName) {
    this(nodeID, networkLocation, hostName, 0L, 0L, 0L, 0);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param capacity capacity of the data node
   * @param dfsUsed space used by the data node
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            int xceiverCount) {
    super(nodeID);
    updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param capacity capacity of the data node, including space used by non-dfs
   * @param dfsUsed the used space by dfs datanode
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID,
                            String networkLocation,
                            String hostName,
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            int xceiverCount) {
    super(nodeID, networkLocation, hostName);
    updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
  }

  /**
   * Add data-node to the block.
   * Add block to the head of the list of blocks belonging to the data-node.
   * 将数据节点加入到block块对应的数据节点列表中
   */
  boolean addBlock(BlockInfo b) {
  	//添加新的数据节点
    if(!b.addNode(this))
      return false;
    // add to the head of the data-node list
    //将此数据块添加到数据节点管理的数据块列表中，并于当前数据块时相邻位置
    blockList = b.listInsert(blockList, this);
    return true;
  }
  
  /**
   * Remove block from the list of blocks belonging to the data-node.
   * Remove data-node from the block.
   */
  boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    return b.removeNode(this);
  }

  /**
   * Move block to the head of the list of blocks belonging to the data-node.
   */
  void moveBlockToHead(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    blockList = b.listInsert(blockList, this);
  }

  void resetBlocks() {
    this.capacity = 0;
    this.remaining = 0;
    this.dfsUsed = 0;
    this.xceiverCount = 0;
    this.blockList = null;
    this.invalidateBlocks.clear();
  }

  public int numBlocks() {
    return blockList == null ? 0 : blockList.listCount(this);
  }

  /**
   */
  void updateHeartbeat(long capacity, long dfsUsed, long remaining,
      int xceiverCount) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.lastUpdate = System.currentTimeMillis();
    this.xceiverCount = xceiverCount;
    rollBlocksScheduled(lastUpdate);
  }

  /**
   * Iterates over the list of blocks belonging to the data-node.
   */
  static private class BlockIterator implements Iterator<Block> {
    private BlockInfo current;
    private DatanodeDescriptor node;
      
    BlockIterator(BlockInfo head, DatanodeDescriptor dn) {
      this.current = head;
      this.node = dn;
    }

    public boolean hasNext() {
      return current != null;
    }

    public BlockInfo next() {
      BlockInfo res = current;
      current = current.getNext(current.findDatanode(node));
      return res;
    }

    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  Iterator<Block> getBlockIterator() {
    return new BlockIterator(this.blockList, this);
  }
  
  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(block, targets);
  }

  /**
   * Store block recovery work.
   */
  void addBlockToBeRecovered(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    recoverBlocks.offer(block, targets);
  }

  /**
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(List<Block> blocklist) {
    assert(blocklist != null && blocklist.size() > 0);
    synchronized (invalidateBlocks) {
      for(Block blk : blocklist) {
        invalidateBlocks.add(blk);
      }
    }
  }

  /**
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    return replicateBlocks.size();
  }

  /**
   * The number of block invalidation items that are pending to 
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }
  
  //与block命令相关的函数
  BlockCommand getReplicationCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = replicateBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blocktargetlist);
  }

  BlockCommand getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = recoverBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_RECOVERBLOCK, blocktargetlist);
  }

  /**
   * Remove the specified number of blocks to be invalidated
   */
  BlockCommand getInvalidateBlocks(int maxblocks) {
    Block[] deleteList = getBlockArray(invalidateBlocks, maxblocks); 
    return deleteList == null? 
        null: new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, deleteList);
  }

  static private Block[] getBlockArray(Collection<Block> blocks, int max) {
    Block[] blockarray = null;
    synchronized(blocks) {
      int available = blocks.size();
      int n = available;
      if (max > 0 && n > 0) {
        if (max < n) {
          n = max;
        }
        // allocate the properly sized block array ... 
        blockarray = new Block[n];

        // iterate tree collecting n blocks... 
        Iterator<Block> e = blocks.iterator();
        int blockCount = 0;

        while (blockCount < n && e.hasNext()) {
          // insert into array ... 
          blockarray[blockCount++] = e.next();

          // remove from tree via iterator, if we are removing 
          // less than total available blocks
          if (n < available){
            e.remove();
          }
        }
        assert(blockarray.length == n);
        
        // now if the number of blocks removed equals available blocks,
        // them remove all blocks in one fell swoop via clear
        if (n == available) { 
          blocks.clear();
        }
      }
    }
    return blockarray;
  }

  void reportDiff(BlocksMap blocksMap,
                  BlockListAsLongs newReport,
                  Collection<Block> toAdd,
                  Collection<Block> toRemove,
                  Collection<Block> toInvalidate) {
    // place a deilimiter in the list which separates blocks 
    // that have been reported from those that have not
    BlockInfo delimiter = new BlockInfo(new Block(), 1);
    boolean added = this.addBlock(delimiter);
    assert added : "Delimiting block cannot be present in the node";
    if(newReport == null)
      newReport = new BlockListAsLongs( new long[0]);
    // scan the report and collect newly reported blocks
    // Note we are taking special precaution to limit tmp blocks allocated
    // as part this block report - which why block list is stored as longs
    Block iblk = new Block(); // a fixed new'ed block to be reused with index i
    Block oblk = new Block(); // for fixing genstamps
    for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
      iblk.set(newReport.getBlockId(i), newReport.getBlockLen(i), 
               newReport.getBlockGenStamp(i));
      BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
      if(storedBlock == null) {
        // if the block with a WILDCARD generation stamp matches 
        // then accept this block.
        // This block has a diferent generation stamp on the datanode 
        // because of a lease-recovery-attempt.
        oblk.set(newReport.getBlockId(i), newReport.getBlockLen(i),
                 GenerationStamp.WILDCARD_STAMP);
        storedBlock = blocksMap.getStoredBlock(oblk);
        if (storedBlock != null && storedBlock.getINode() != null &&
            (storedBlock.getGenerationStamp() <= iblk.getGenerationStamp() ||
             storedBlock.getINode().isUnderConstruction())) {
          // accept block. It wil be cleaned up on cluster restart.
        } else {
          storedBlock = null;
        }
      }
      if(storedBlock == null) {
        // If block is not in blocksMap it does not belong to any file
        toInvalidate.add(new Block(iblk));
        continue;
      }
      if(storedBlock.findDatanode(this) < 0) {// Known block, but not on the DN
        // if the size differs from what is in the blockmap, then return
        // the new block. addStoredBlock will then pick up the right size of this
        // block and will update the block object in the BlocksMap
        if (storedBlock.getNumBytes() != iblk.getNumBytes()) {
          toAdd.add(new Block(iblk));
        } else {
          toAdd.add(storedBlock);
        }
        continue;
      }
      // move block to the head of the list
      this.moveBlockToHead(storedBlock);
    }
    // collect blocks that have not been reported
    // all of them are next to the delimiter
    Iterator<Block> it = new BlockIterator(delimiter.getNext(0), this);
    while(it.hasNext()) {
      BlockInfo storedBlock = (BlockInfo)it.next();
      INodeFile file = storedBlock.getINode();
      if (file == null || !file.isUnderConstruction()) {
        toRemove.add(storedBlock);
      }
    }
    this.removeBlock(delimiter);
  }

  /** Serialization for FSEditLog */
  void readFieldsFromFSEditLog(DataInput in) throws IOException {
    this.name = UTF8.readString(in);
    this.storageID = UTF8.readString(in);
    this.infoPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostName = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }
  
  /**
   * @return Approximate number of blocks currently scheduled to be written 
   * to this datanode.
   */
  public int getBlocksScheduled() {
    return currApproxBlocksScheduled + prevApproxBlocksScheduled;
  }
  
  /**
   * Increments counter for number of blocks scheduled. 
   */
  void incBlocksScheduled() {
    currApproxBlocksScheduled++;
  }
  
  /**
   * Decrements counter for number of blocks scheduled.
   * 每次做block调度完成后，计数量要相应减去1
   */
  void decBlocksScheduled() {
    if (prevApproxBlocksScheduled > 0) {
      prevApproxBlocksScheduled--;
    } else if (currApproxBlocksScheduled > 0) {
      currApproxBlocksScheduled--;
    } 
    // its ok if both counters are zero.
  }
  
  /**
   * Adjusts curr and prev number of blocks scheduled every few minutes.
   */
  private void rollBlocksScheduled(long now) {
    if ((now - lastBlocksScheduledRollTime) > 
        BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled = currApproxBlocksScheduled;
      currApproxBlocksScheduled = 0;
      lastBlocksScheduledRollTime = now;
    }
  }
  
  class DecommissioningStatus {
    int underReplicatedBlocks;
    int decommissionOnlyReplicas;
    int underReplicatedInOpenFiles;
    long startTime;

    synchronized void set(int underRep, int onlyRep, int underConstruction) {
      if (isDecommissionInProgress() == false) {
        return;
      }
      underReplicatedBlocks = underRep;
      decommissionOnlyReplicas = onlyRep;
      underReplicatedInOpenFiles = underConstruction;
    }

    synchronized int getUnderReplicatedBlocks() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedBlocks;
    }

    synchronized int getDecommissionOnlyReplicas() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return decommissionOnlyReplicas;
    }

    synchronized int getUnderReplicatedInOpenFiles() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedInOpenFiles;
    }

    synchronized void setStartTime(long time) {
      startTime = time;
    }

    synchronized long getStartTime() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return startTime;
    }
  } // End of class DecommissioningStatus
  
  /**
   * @return Blanacer bandwidth in bytes per second for this datanode.
   */
  public long getBalancerBandwidth() {
    return this.bandwidth;
  }
  
  /**
   * @param bandwidth Blanacer bandwidth in bytes per second for this datanode.
   */
  public void setBalancerBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  boolean firstBlockReport() {
    return firstBlockReport;
  }
  
  void processedBlockReport() {
    firstBlockReport = false;
  }
}
