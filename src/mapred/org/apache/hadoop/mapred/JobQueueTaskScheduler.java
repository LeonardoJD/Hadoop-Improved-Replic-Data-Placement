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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * A {@link TaskScheduler} that keeps jobs in a queue in priority order (FIFO
 * by default).
 */
class JobQueueTaskScheduler extends TaskScheduler {
  
  private static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
  public static final Log LOG = LogFactory.getLog(JobQueueTaskScheduler.class);
  
  protected JobQueueJobInProgressListener jobQueueJobInProgressListener;
  protected EagerTaskInitializationListener eagerTaskInitializationListener;
  private float padFraction;
  
  public JobQueueTaskScheduler() {
    this.jobQueueJobInProgressListener = new JobQueueJobInProgressListener();
  }
  
  @Override
  public synchronized void start() throws IOException {
    super.start();
    taskTrackerManager.addJobInProgressListener(jobQueueJobInProgressListener);
    eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
    eagerTaskInitializationListener.start();
    taskTrackerManager.addJobInProgressListener(
        eagerTaskInitializationListener);
  }
  
  @Override
  public synchronized void terminate() throws IOException {
    if (jobQueueJobInProgressListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          jobQueueJobInProgressListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    padFraction = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                 0.01f);
    this.eagerTaskInitializationListener =
      new EagerTaskInitializationListener(conf);
  }
  /**
   * 给一个计算节点分配若干任务
   */
  @Override
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {
    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus(); 
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    //整个集群的TaskTrackers
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    //整个集群的Map容量
    final int clusterMapCapacity = clusterStatus.getMaxMapTasks();
    //整个集群的Reduce容量
    final int clusterReduceCapacity = clusterStatus.getMaxReduceTasks();
    //当前集群待调度的作业队列
    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    //
    // Get map + reduce counts for the current tracker.
    //
    final int trackerMapCapacity = taskTrackerStatus.getMaxMapSlots();
    final int trackerReduceCapacity = taskTrackerStatus.getMaxReduceSlots();
    final int trackerRunningMaps = taskTrackerStatus.countMapTasks();
    final int trackerRunningReduces = taskTrackerStatus.countReduceTasks();
    
    //添加的关于CPU平均负载的计算公式：如果TaskTracker_CPU_NOW > TaskTracker_CPU*cluster_CPU_NOW/cluster_CPU
    clusterStatus.getActiveTrackerNames();
    // 用来存储给当前节点分配的任务
    List<Task> assignedTasks = new ArrayList<Task>();
  /*  if (taskTrackerStatus.getResourceStatus().getCpuUsage()>85.0) {
		assignedTasks = null;
		return assignedTasks;
	}*/
    //
    // Compute (running + pending) map and reduce task numbers across pool
    //
    int remainingReduceLoad = 0;//当前集群执行reduce尚需的计算能力
    int remainingMapLoad = 0;//当前集群执行map尚需的计算能力
    synchronized (jobQueue) {
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          remainingMapLoad += (job.desiredMaps() - job.finishedMaps());
          //当前作业是否可以开始调度reduce任务：map任务是否执行大于5%
          if (job.scheduleReduces()) {
            remainingReduceLoad += 
              (job.desiredReduces() - job.finishedReduces());
          }
        }
      }
    }

    // 计算当前map任务的负载
    double mapLoadFactor = 0.0;
    if (clusterMapCapacity > 0) {
      mapLoadFactor = (double)remainingMapLoad / clusterMapCapacity;
    }
    //计算当前reduce任务的负载
    double reduceLoadFactor = 0.0;
    if (clusterReduceCapacity > 0) {
      reduceLoadFactor = (double)remainingReduceLoad / clusterReduceCapacity;
    }
          
    //
    // In the below steps, we allocate first map tasks (if appropriate),
    // and then reduce tasks if appropriate.  We go through all jobs
    // in order of job arrival; jobs only get serviced if their 
    // predecessors are serviced, too.
    //

    //
    // We assign tasks to the current taskTracker if the given machine 
    // has a workload that's less than the maximum load of that kind of
    // task.
    // However, if the cluster is close to getting loaded i.e. we don't
    // have enough _padding_ for speculative executions etc., we only 
    // schedule the "highest priority" task i.e. the task from the job 
    // with the highest priority.
    //
    //Math.ceil()向上取整
    /**
     * //为了尽可能使得每一个TaskTracker获得相同的Capacity，并保证在供需平衡，所以这里用到了mapLoadFactor来平衡，
     * 例如当前平均每个TaskTracker需要调度的MapTask的个数经过mapLoadFactor * trackerMapCapacity = 3， 
     * 而trackerMapCapacity=4，则这个时候，当前map的容量是3，这样使得任务可以完成，还要保证每一个TaskTracker
     * 不至于出现TaskSkew现象。
     */
    final int trackerCurrentMapCapacity = 
      Math.min((int)Math.ceil(mapLoadFactor * trackerMapCapacity), 
                              trackerMapCapacity);
    //final int trackerCurrentMapCapacity =  trackerMapCapacity;
    int availableMapSlots = trackerCurrentMapCapacity - trackerRunningMaps;
    boolean exceededMapPadding = false;
    if (availableMapSlots > 0) {
      exceededMapPadding = 
        exceededPadding(true, clusterStatus, trackerMapCapacity);
    }
    
    int numLocalMaps = 0;
    int numNonLocalMaps = 0;
    scheduleMaps:
    for (int i=0; i < availableMapSlots; ++i) {
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          //首先满足已经运行，并且map task还么有部署的作业
          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }

          Task t = null;
          
          // Try to schedule a node-local or rack-local Map task
          t = 
            job.obtainNewNodeOrRackLocalMapTask(taskTrackerStatus, 
                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
          if (t != null) {
            assignedTasks.add(t);
            ++numLocalMaps;
            
            // Don't assign map tasks to the hilt!
            // Leave some free slots in the cluster for future task-failures,
            // speculative tasks etc. beyond the highest priority job
            if (exceededMapPadding) {
              break scheduleMaps;
            }
           
            // Try all jobs again for the next Map task 
            break;
          }
          
          // Try to schedule a node-local or rack-local Map task
          //尝试给当前计算节点分配一个非本地map任务，如果分配成功则结束对该节点的map任务分配，  
          //以避免它抢走了其它计算节点的本地map任务 
          t = 
            job.obtainNewNonLocalMapTask(taskTrackerStatus, numTaskTrackers,
                                   taskTrackerManager.getNumberOfUniqueHosts());
          
          if (t != null) {
            assignedTasks.add(t);
            ++numNonLocalMaps;
            
            // We assign at most 1 off-switch or speculative task
            // This is to prevent TaskTrackers from stealing local-tasks
            // from other TaskTrackers.
            break scheduleMaps;
          }
        }
      }
    }
    int assignedMaps = assignedTasks.size();

    //
    // Same thing, but for reduce tasks
    // However we _never_ assign more than 1 reduce task per heartbeat
    //
    final int trackerCurrentReduceCapacity = 
      Math.min((int)Math.ceil(reduceLoadFactor * trackerReduceCapacity), 
               trackerReduceCapacity);
    final int availableReduceSlots = 
      Math.min((trackerCurrentReduceCapacity - trackerRunningReduces), 1);
    boolean exceededReducePadding = false;
    if (availableReduceSlots > 0) {
      exceededReducePadding = exceededPadding(false, clusterStatus, 
                                              trackerReduceCapacity);
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
              job.numReduceTasks == 0) {
            continue;
          }

          Task t = 
            job.obtainNewReduceTask(taskTrackerStatus, numTaskTrackers, 
                                    taskTrackerManager.getNumberOfUniqueHosts()
                                    );
          if (t != null) {
            assignedTasks.add(t);
            break;
          }
          
          // Don't assign reduce tasks to the hilt!
          // Leave some free slots in the cluster for future task-failures,
          // speculative tasks etc. beyond the highest priority job
          //需要当前计算机节点保留部分reduce计算能力，所以结束当前节点的map任务
          if (exceededReducePadding) {
            break;
          }
        }
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Task assignments for " + taskTrackerStatus.getTrackerName() + " --> " +
                "[" + mapLoadFactor + ", " + trackerMapCapacity + ", " + 
                trackerCurrentMapCapacity + ", " + trackerRunningMaps + "] -> [" + 
                (trackerCurrentMapCapacity - trackerRunningMaps) + ", " +
                assignedMaps + " (" + numLocalMaps + ", " + numNonLocalMaps + 
                ")] [" + reduceLoadFactor + ", " + trackerReduceCapacity + ", " + 
                trackerCurrentReduceCapacity + "," + trackerRunningReduces + 
                "] -> [" + (trackerCurrentReduceCapacity - trackerRunningReduces) + 
                ", " + (assignedTasks.size()-assignedMaps) + "]");
    }

    return assignedTasks;
  }

  public synchronized List<Task> assignTasks1(TaskTracker taskTracker,double trackerCpuNow,double cpuThreashold)
	      throws IOException {
	  //添加一开始的判断条件，如果此条件不满足，直接跳过
	 /* if ((trackerCpuNow>90)&&(trackerCpuNow>cpuThreashold)) {
		    System.out.println("datanode"+(taskTracker.getTrackerName()).charAt(16)+
		    		"trackerCpuNow"+trackerCpuNow+
		    		"cpuThreashold"+cpuThreashold
		    		);
			return null;   
			}*/
	  /* if (trackerCpuNow<cpuThreashold) {
	    System.out.println("datanode"+(taskTracker.getTrackerName()).charAt(16)+
	    		"AvailablePhysicalMemory"+trackerCpuNow+
	    		"memoryThreashold"+cpuThreashold
	    		);
		return null;   
		}*/
	  ///////////////////////////////////////////////////////////////////////////////////////////////
	    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus(); 
	    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
	    //整个集群的TaskTrackers
	    final int numTaskTrackers = clusterStatus.getTaskTrackers();
	    //整个集群的Map容量
	    final int clusterMapCapacity = clusterStatus.getMaxMapTasks();
	    //整个集群的Reduce容量
	    final int clusterReduceCapacity = clusterStatus.getMaxReduceTasks();
	    //当前集群待调度的作业队列
	    Collection<JobInProgress> jobQueue =
	      jobQueueJobInProgressListener.getJobQueue();

	    //
	    // Get map + reduce counts for the current tracker.
	    //
	    final int trackerMapCapacity = taskTrackerStatus.getMaxMapSlots();
	    final int trackerReduceCapacity = taskTrackerStatus.getMaxReduceSlots();
	    final int trackerRunningMaps = taskTrackerStatus.countMapTasks();
	    final int trackerRunningReduces = taskTrackerStatus.countReduceTasks();
	    
	    //添加的关于CPU平均负载的计算公式：如果TaskTracker_CPU_NOW > TaskTracker_CPU*cluster_CPU_NOW/cluster_CPU
	    clusterStatus.getActiveTrackerNames();
	    // 用来存储给当前节点分配的任务
	    List<Task> assignedTasks = new ArrayList<Task>();
	  /*  if (taskTrackerStatus.getResourceStatus().getCpuUsage()>85.0) {
			assignedTasks = null;
			return assignedTasks;
		}*/
	    //
	    // Compute (running + pending) map and reduce task numbers across pool
	    //
	    int remainingReduceLoad = 0;//当前集群执行reduce尚需的计算能力
	    int remainingMapLoad = 0;//当前集群执行map尚需的计算能力
	    synchronized (jobQueue) {
	      for (JobInProgress job : jobQueue) {
	        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
	          remainingMapLoad += (job.desiredMaps() - job.finishedMaps());
	          //当前作业是否可以开始调度reduce任务：map任务是否执行大于5%
	          if (job.scheduleReduces()) {
	            remainingReduceLoad += 
	              (job.desiredReduces() - job.finishedReduces());
	          }
	        }
	      }
	    }

	    // 计算当前map任务的负载
	    double mapLoadFactor = 0.0;
	    if (clusterMapCapacity > 0) {
	      mapLoadFactor = (double)remainingMapLoad / clusterMapCapacity;
	    }
	    //计算当前reduce任务的负载
	    double reduceLoadFactor = 0.0;
	    if (clusterReduceCapacity > 0) {
	      reduceLoadFactor = (double)remainingReduceLoad / clusterReduceCapacity;
	    }
	        
	    //
	    // In the below steps, we allocate first map tasks (if appropriate),
	    // and then reduce tasks if appropriate.  We go through all jobs
	    // in order of job arrival; jobs only get serviced if their 
	    // predecessors are serviced, too.
	    //

	    //
	    // We assign tasks to the current taskTracker if the given machine 
	    // has a workload that's less than the maximum load of that kind of
	    // task.
	    // However, if the cluster is close to getting loaded i.e. we don't
	    // have enough _padding_ for speculative executions etc., we only 
	    // schedule the "highest priority" task i.e. the task from the job 
	    // with the highest priority.
	    //
	    //Math.ceil()向上取整
	    /**
	     * //为了尽可能使得每一个TaskTracker获得相同的Capacity，并保证在供需平衡，所以这里用到了mapLoadFactor来平衡，
	     * 例如当前平均每个TaskTracker需要调度的MapTask的个数经过mapLoadFactor * trackerMapCapacity = 3， 
	     * 而trackerMapCapacity=4，则这个时候，当前map的容量是3，这样使得任务可以完成，还要保证每一个TaskTracker
	     * 不至于出现TaskSkew现象。
	     */    
	  /* final int trackerCurrentMapCapacity =  
	      Math.min((int)Math.ceil(mapLoadFactor * trackerMapCapacity), 
	                              trackerMapCapacity);*/
	    //对于动态slot的策略，不考虑负载均衡的问题，只考虑资源利用率的问题
	  final int trackerCurrentMapCapacity = trackerMapCapacity;
	    int availableMapSlots = trackerCurrentMapCapacity - trackerRunningMaps;
	    System.out.println("Node:"+taskTracker.getTrackerName().charAt(16)
	    		+" trackerCurrentMapCapacity:"+trackerCurrentMapCapacity
	    		+" trackerRunningMaps:"+trackerRunningMaps
	    		+" availableMapSlots:"+availableMapSlots);
	    boolean exceededMapPadding = false;  
	    if (availableMapSlots > 0) {       
	      exceededMapPadding = 
	        exceededPadding(true, clusterStatus, trackerMapCapacity);
	    }
	    
	  
	    int numLocalMaps = 0;
	    int numNonLocalMaps = 0;
	    scheduleMaps:
	    for (int i=0; i < availableMapSlots; ++i) {
	      synchronized (jobQueue) {
	        for (JobInProgress job : jobQueue) {
	          //首先满足已经运行，并且map task还么有部署的作业
	          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
	            continue;
	          }

	          Task t = null;
	          
	          // Try to schedule a node-local or rack-local Map task
	          t = 
	            job.obtainNewNodeOrRackLocalMapTask(taskTrackerStatus, 
	                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
	          if (t != null) {
	            assignedTasks.add(t);
	            ++numLocalMaps;
	            
	            // Don't assign map tasks to the hilt!
	            // Leave some free slots in the cluster for future task-failures,
	            // speculative tasks etc. beyond the highest priority job
	            if (exceededMapPadding) {
	              break scheduleMaps;
	            }
	           
	            // Try all jobs again for the next Map task 
	            break;
	          }
	          
	          // Try to schedule a node-local or rack-local Map task
	          //尝试给当前计算节点分配一个非本地map任务，如果分配成功则结束对该节点的map任务分配，  
	          //以避免它抢走了其它计算节点的本地map任务 
	          t = 
	            job.obtainNewNonLocalMapTask(taskTrackerStatus, numTaskTrackers,
	                                   taskTrackerManager.getNumberOfUniqueHosts());
	          
	          if (t != null) {
	            assignedTasks.add(t);
	            ++numNonLocalMaps;
	            
	            // We assign at most 1 off-switch or speculative task
	            // This is to prevent TaskTrackers from stealing local-tasks
	            // from other TaskTrackers.
	            break scheduleMaps;
	          }
	        }
	      }
	    }
	    int assignedMaps = assignedTasks.size();

	    //
	    // Same thing, but for reduce tasks
	    // However we _never_ assign more than 1 reduce task per heartbeat
	    //
	    final int trackerCurrentReduceCapacity = 
	      Math.min((int)Math.ceil(reduceLoadFactor * trackerReduceCapacity), 
	               trackerReduceCapacity);
	    final int availableReduceSlots = 
	      Math.min((trackerCurrentReduceCapacity - trackerRunningReduces), 1);
	    boolean exceededReducePadding = false;
	    if (availableReduceSlots > 0) {
	      exceededReducePadding = exceededPadding(false, clusterStatus, 
	                                              trackerReduceCapacity);
	      synchronized (jobQueue) {
	        for (JobInProgress job : jobQueue) {
	          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
	              job.numReduceTasks == 0) {
	            continue;
	          }

	          Task t = 
	            job.obtainNewReduceTask(taskTrackerStatus, numTaskTrackers, 
	                                    taskTrackerManager.getNumberOfUniqueHosts()
	                                    );
	          if (t != null) {
	            assignedTasks.add(t);
	            break;
	          }
	          
	          // Don't assign reduce tasks to the hilt!
	          // Leave some free slots in the cluster for future task-failures,
	          // speculative tasks etc. beyond the highest priority job
	          //需要当前计算机节点保留部分reduce计算能力，所以结束当前节点的map任务
	          if (exceededReducePadding) {
	            break;
	          }
	        }
	      }
	    }
	    
	    if (LOG.isDebugEnabled()) {
	      LOG.debug("Task assignments for " + taskTrackerStatus.getTrackerName() + " --> " +
	                "[" + mapLoadFactor + ", " + trackerMapCapacity + ", " + 
	                trackerCurrentMapCapacity + ", " + trackerRunningMaps + "] -> [" + 
	                (trackerCurrentMapCapacity - trackerRunningMaps) + ", " +
	                assignedMaps + " (" + numLocalMaps + ", " + numNonLocalMaps + 
	                ")] [" + reduceLoadFactor + ", " + trackerReduceCapacity + ", " + 
	                trackerCurrentReduceCapacity + "," + trackerRunningReduces + 
	                "] -> [" + (trackerCurrentReduceCapacity - trackerRunningReduces) + 
	                ", " + (assignedTasks.size()-assignedMaps) + "]");
	    }

	    return assignedTasks;
	  }
  private boolean exceededPadding(boolean isMapTask, 
                                  ClusterStatus clusterStatus, 
                                  int maxTaskTrackerSlots) { 
    int numTaskTrackers = clusterStatus.getTaskTrackers();
    int totalTasks = 
      (isMapTask) ? clusterStatus.getMapTasks() : 
        clusterStatus.getReduceTasks();
    int totalTaskCapacity = 
      isMapTask ? clusterStatus.getMaxMapTasks() : 
                  clusterStatus.getMaxReduceTasks();

    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    boolean exceededPadding = false;
    synchronized (jobQueue) {
      int totalNeededTasks = 0;
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() != JobStatus.RUNNING ||
            job.numReduceTasks == 0) {
          continue;
        }

        //
        // Beyond the highest-priority task, reserve a little 
        // room for failures and speculative executions; don't 
        // schedule tasks to the hilt.
        //
        totalNeededTasks += 
          isMapTask ? job.desiredMaps() : job.desiredReduces();
        int padding = 0;
        if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
          padding = 
            Math.min(maxTaskTrackerSlots,
                     (int) (totalNeededTasks * padFraction));
        }
        if (totalTasks + padding >= totalTaskCapacity) {
          exceededPadding = true;
          break;
        }
      }
    }

    return exceededPadding;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    return jobQueueJobInProgressListener.getJobQueue();
  }  
}
