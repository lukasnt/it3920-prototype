package com.lukasnt.spark.experiments

case class ExperimentMemoryInfo(
    // In Bytes
    driverTotalMemory: Long,
    driverMemoryFree: Long,
    driverMemoryUsed: Long,
    totalSparkExecutorMemoryAllocated: Long,
    totalSparkExecutorMemoryFree: Long,
    totalSparkExecutorMemoryUsed: Long,
    totalRDDMemorySize: Long,
    totalRDDDiskSize: Long,
    totalMemoryUsed: Long,
    // In MB
    driverMemoryUsedMB: Int,
    totalSparkExecutorMemoryUsedMB: Int,
    totalRDDMemorySizeMB: Int,
    totalRDDDiskSizeMB: Int,
    totalMemoryUsedMB: Int
) {
  override def toString: String = {
    s"Driver Total Memory: $driverTotalMemory Bytes\n" +
      s"Driver Memory Free: $driverMemoryFree Bytes\n" +
      s"Driver Memory Used: $driverMemoryUsed Bytes\n" +
      s"Total Spark Executor Memory Allocated: $totalSparkExecutorMemoryAllocated Bytes\n" +
      s"Total Spark Executor Memory Free: $totalSparkExecutorMemoryFree Bytes\n" +
      s"Total Spark Executor Memory Used: $totalSparkExecutorMemoryUsed Bytes\n" +
      s"Total RDD Memory Size: $totalRDDMemorySize Bytes\n" +
      s"Total RDD Disk Size: $totalRDDDiskSize Bytes\n" +
      s"Total Memory Used: $totalMemoryUsed Bytes\n" +
      s"Driver Memory Used: $driverMemoryUsedMB MB\n" +
      s"Total Spark Executor Memory Used: $totalSparkExecutorMemoryUsedMB MB\n" +
      s"Total RDD Memory Size: $totalRDDMemorySizeMB MB\n" +
      s"Total RDD Disk Size: $totalRDDDiskSizeMB MB\n" +
      s"Total Memory Used: $totalMemoryUsedMB MB"
  }
}
