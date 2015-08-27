# hadoop-hdfs
Hadoop分布式文件系统hdfs代码分析

## 目录介绍
### Datanode-数据块基本结构
主要介绍了HDFS中第二关系块结构，数据块到数据节点的映射关系。[详细介绍链接](http://blog.csdn.net/androidlushangderen/article/details/47734269)

### Decommision-节点撤销
主要介绍了数据节点下线撤销机制。[详细介绍链接](http://blog.csdn.net/androidlushangderen/article/details/47788227)

### INode-文件目录结构
主要介绍了文件系统中文件目录节点结构，相关类。[详细介绍链接](http://blog.csdn.net/androidlushangderen/article/details/47427925)

### Lease-租约
主要介绍了分布式文件系统中与文件租约相关的内容。[详细介绍链接](http://blog.csdn.net/androidlushangderen/article/details/48012001)

### NameMetaData-元数据机制
主要介绍了namenode上元数据相关操作机制，包括editlog和fsimage2个文件在此期间的状态改变。[详细介绍链接](http://blog.csdn.net/androidlushangderen/article/details/47679977)

### Register-数据节点注册
主要介绍了数据节点启动时的注册操作。[详细介绍链接](http://blog.csdn.net/androidlushangderen/article/details/47945597)
