%scala
sc.setJobDescription("Step 0-2: Register spill listener")
// Secondary source https://www.databricks.training/spark-ui-simulator/experiment-6518/v002-S/index.html
// Original source https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/TestUtils.scala#L422

class SpillListener extends org.apache.spark.scheduler.SparkListener {
  import org.apache.spark.scheduler.{SparkListenerTaskEnd,SparkListenerStageCompleted}
  import org.apache.spark.executor.TaskMetrics
  import scala.collection.mutable.{HashMap,ArrayBuffer}
  
  private val stageIdToTaskMetrics = new HashMap[Int, ArrayBuffer[TaskMetrics]] // Mutable hashmap
  private val spilledStageIds = new scala.collection.mutable.HashSet[Int] // Non-mutable hashmap
  
  def numSpilledStages: Int = synchronized {spilledStageIds.size} // Keep track of number of spilled stages
  def reset(): Unit = synchronized { spilledStageIds.clear } // Reset function to reset spill listener
  def report(): Unit = synchronized { println(f"Spilled Stages:  ${numSpilledStages}%,d") } // Print how many stages have been spilled
  // Below two functions are actual implementation
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {stageIdToTaskMetrics.getOrElseUpdate(taskEnd.stageId, new ArrayBuffer[TaskMetrics]) += taskEnd.taskMetrics} // Updates hashmaps above - keep track of stages that have spilled
  // Runs when stage has completed, calculates whether there's spill or not and reports it
  override def onStageCompleted(stageComplete:  SparkListenerStageCompleted): Unit = synchronized {
    val stageId = stageComplete.stageInfo.stageId
    val metrics = stageIdToTaskMetrics.remove(stageId).toSeq.flatten
    val spilled = metrics.map(_.memoryBytesSpilled).sum > 0
    if (spilled) spilledStageIds += stageId
  }
}
val spillListener = new SpillListener() // Instantiate spill listener
sc.addSparkListener(spillListener) // Add it to spark context
