/*******************************************************************************
 * Libera HyperVisor development based OpenVirteX for SDN 2.0
 *
 *   Physical Network Monitoring Framework
 *
 * This is created by Libera Project team in Korea University
 *
 * Author(s): Gyeongsik Yang, Wontae Jeong, Da-hyun Jeon
 ******************************************************************************/
package net.onrc.openvirtex.elements.network;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.onrc.openvirtex.elements.datapath.OVXSwitch;
import net.onrc.openvirtex.elements.datapath.statistics.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.elements.datapath.statistics.BasicMonitoringEntity.EntityType;
import net.onrc.openvirtex.exceptions.IndexOutOfBoundException;
import net.onrc.openvirtex.messages.statistics.OVXFlowStatsRequest;
import net.onrc.openvirtex.util.BitSetIndex;
import net.onrc.openvirtex.util.BitSetIndex.IndexType;

public final class PhysicalNetworkScheduler {
	Logger log = LogManager.getLogger(PhysicalNetworkScheduler.class.getName());

	// 50%: 0, 90%: 1.645, 95%: 1.96, 99%: 2.575
	//private double k1 = 1.645, k2 = 1.96;
	private double k1 = 1.96, k2 = 2.575;

	private double mergeThresh = 0.3; //
	private final long intervalSize;
	private long timeMapSize;
	private long gcd;
	private final long timeSlotSize;
	private final int iterationNum = 3;
	
	private final HashMap<Long, MonitoringPool> poolMap;
	private final HashMap<PhysicalSwitch, Long> aggFlowMap;
	private final HashMap<PhysicalSwitch, Long> aggPortMap; 

	// unified HashMap for all types of tasks
	ConcurrentHashMap<Integer, ArrayList<Long>> ExecTimeMap = new ConcurrentHashMap<Integer, ArrayList<Long>>();
	ConcurrentHashMap<Integer, Integer>TimeResourceMap = new ConcurrentHashMap<Integer, Integer>();

	ConcurrentHashMap<PhysicalSwitch, Integer> singleFlowCounter = new ConcurrentHashMap<PhysicalSwitch, Integer>();
	ConcurrentHashMap<PhysicalSwitch, Integer> singlePortCounter = new ConcurrentHashMap<PhysicalSwitch, Integer>();
	ConcurrentHashMap<PhysicalSwitch, Integer> aggFlowCounter = new ConcurrentHashMap<PhysicalSwitch, Integer>();
	ConcurrentHashMap<PhysicalSwitch, Integer> aggPortCounter = new ConcurrentHashMap<PhysicalSwitch, Integer>();

	ConcurrentHashMap<PhysicalSwitch, ArrayList<SingleFlowStatMonitor>> singleFlowJobMap = new ConcurrentHashMap<PhysicalSwitch, ArrayList<SingleFlowStatMonitor>>();
	ConcurrentHashMap<PhysicalSwitch, ArrayList<VirtualPortStatMonitor>> singlePortJobMap = new ConcurrentHashMap<PhysicalSwitch, ArrayList<VirtualPortStatMonitor>>();
	ConcurrentHashMap<PhysicalSwitch, ArrayList<FlowStatMonitor>> FlowJobMap = new ConcurrentHashMap<PhysicalSwitch, ArrayList<FlowStatMonitor>>();
	ConcurrentHashMap<PhysicalSwitch, ArrayList<PortStatMonitor>> PortJobMap = new ConcurrentHashMap<PhysicalSwitch, ArrayList<PortStatMonitor>>();

	ConcurrentHashMap<Long, ScheduledFuture<?>> SchedMap = new ConcurrentHashMap<>();


	public PhysicalNetworkScheduler(long size){
		this.intervalSize = size;
		this.timeMapSize = this.intervalSize;
		this.timeSlotSize = this.intervalSize;
		this.poolMap = new HashMap<>();
		this.aggFlowMap = new HashMap<>();
		this.aggPortMap = new HashMap<>();
	}
	
	private class MonitoringPool{
		private long poolId;
		private long nextExecution; 		// absolute time for execution
		private long interval;
		private Integer idx; 				// nextExecution % intervalSize (0~99, when intervalSize = 100)
		//private ConcurrentHashMap<Long, ScheduledFuture<?>> SchedMap;
		private ScheduledFuture<?> schd;
				
		private final ScheduledThreadPoolExecutor pool;
		int corePoolSize = 100; // the size of threads
		
		//set interval, add interval, and update nextExecution
		public MonitoringPool(final long poolId, long interval){
			Date date = new Date();
			this.poolId = poolId;
			this.interval = interval;
			this.nextExecution = date.getTime() + interval;
			this.idx = (int)(nextExecution % timeMapSize);
			//this.idx = (int)(nextExecution % intervalSize);
			
			//log.info("[MonitoringPool] ID : {}, interval : {}, next : {}, idx: {}", poolId, interval, nextExecution, idx);
			
			this.pool = new ScheduledThreadPoolExecutor(corePoolSize);
			//this.SchedMap = new ConcurrentHashMap<>();
			//this.schd = this.pool.scheduleAtFixedRate(new PoolManager(this), 0, interval, TimeUnit.MILLISECONDS);
		}
		
		/*
		public void aggJobSchedule(BasicMonitoringEntity task, long poolID){
			if(task.getType() == BasicMonitoringEntity.EntityType.SINGLE_FLOW_STAT || 
					task.getType() == BasicMonitoringEntity.EntityType.SINGLE_PORT_STAT){
				log.info("");
				return;
			}
		}
		*/
		
		public void schedule(BasicMonitoringEntity task){
			task.startPreMonitoring();

			increaseCounter(task);
			addJobMap(task);

			Date date = new Date();
			Integer resource = task.UpdateAndGetJobSize();
			//log.info("Retrieved job size for task {}: {}", task.toString(), resource);

			long delay = 0;
			switch(task.getType()){
				case SINGLE_FLOW_STAT:
					delay = interval * 95 / 100;
				case SINGLE_PORT_STAT:
					delay = interval * 95 / 100;
				case AGG_FLOW_STAT:
					//setExecTime(date, resource);
					delay = interval / 2;
				default:
					break;
			}

//			setExecTime(date, resource);

			SchedMap.put(task.getID(), pool.scheduleAtFixedRate(task, delay, interval, TimeUnit.MILLISECONDS));

//			SchedMap.put(task.getID(), pool.scheduleAtFixedRate(task, (this.nextExecution-date.getTime()), interval, TimeUnit.MILLISECONDS));
			//log.info("SchedMap.retrieving: {}", SchedMap.get(task.getID()).toString());
			//schd = pool.scheduleAtFixedRate(task, (this.nextExecution-date.getTime()), interval, TimeUnit.MILLISECONDS);
			//log.info("[schedule] delay: {}, interval: {}, next: {}, now: {}", (this.nextExecution-date.getTime()), interval, this.nextExecution, date.getTime());
			//log.info("============ POOL\n {}", pool.toString());

			task.setPoolID(this.poolId);
		}
	
		// [dhjeon] modifying...
		public synchronized void deScheduleForSwitch(BasicMonitoringEntity task){
			//log.info("[deScheduleForSwitch]");

			PhysicalSwitch sw = task.getPhysicalSwtich();
			//log.info("sw: {}", sw.getName());
			switch (task.getType()){
				case SINGLE_FLOW_STAT:
					Iterator<SingleFlowStatMonitor> itrf = singleFlowJobMap.get(sw).iterator();
					//Iterator<FlowStatMonitor> itrf = FlowJobMap.get(sw).iterator();
					//log.info("[deScheduleForSwitch] flow job map {}", FlowJobMap.get(sw));


					if(singleFlowJobMap.get(sw)!=null && itrf!=null) {
						while (itrf.hasNext()) {
							SingleFlowStatMonitor monitor = itrf.next();
							if (monitor.getScheduleStatus()) {
								monitor.endPreMonitoring();
								this.deScheduleforAggregatedTask(monitor);
								itrf.remove();
							}
							//this.deSchedule((BasicMonitoringEntity) itrf);
							//task.endPreMonitoring();
						}
					}


				break;
			case SINGLE_PORT_STAT:
				//Iterator<PortStatMonitor> itrp = PortJobMap.get(sw).iterator();
				//log.info("[deScheduleForSwitch] port job map {}", PortJobMap.get(sw));
				/*
				while(itrp.hasNext()){
					this.deSchedule((BasicMonitoringEntity) itrp);
				}
				break;
				*/
			default:
				return;
		}
		}
		
		public synchronized void deSchedule(BasicMonitoringEntity task){
			log.info("[deSchedule] removing --- poolID {} idx {}", task.getPoolID(), idx);

			// Should update ReourceTImeMap - to be debugged
			int resource = task.GetJobSize();
			long step = interval / iterationNum;
			//log.info ("Deschedule routine - retrived job size: {}", resource);
			for(int j=this.idx-(int)step; j >= 0; j=j-(int)step){
				if(TimeResourceMap.get(j) != null) {
					TimeResourceMap.put(j, TimeResourceMap.get(j) - resource);
				}
				//log.info ("Update resource usage - {} interval, {}", j, TimeResourceMap.get(j));

			}

			for(int j = this.idx; j < timeMapSize; j=j+(int)step) {
				if(TimeResourceMap.get(j) != null) {
					TimeResourceMap.put(j, TimeResourceMap.get(j) - resource);
				}
				//log.info ("Update resource usage - {} interval, {}", j, TimeResourceMap.get(j));
			}

			task.endPreMonitoring();
			decreaseCounter(task);
			removeJobMap(task);
			ExecTimeMap.remove(idx);

			//idx = null;

			this.pool.remove(task);
			SchedMap.get(task.getID()).cancel(false);
			//this.schd.cancel(false);	// job thread removing
			
			task.setPoolID((long) 0.0);
			//printExecTimeMap();
			
			log.info("[deSchedule] removed taskid: {} --- poolID {} idx {}", task.getID(), task.getPoolID(), idx);
		}



		public synchronized void deScheduleforAggregatedTask(BasicMonitoringEntity task){
			log.info("[deSchedule] removing --- poolID {} idx {}", task.getPoolID(), idx);

			// Should update ReourceTImeMap - to be debugged
			int resource = task.GetJobSize();
			long step = interval / iterationNum;
			//log.info ("Deschedule routine - retrived job size: {}", resource);
			for(int j=this.idx-(int)step; j >= 0; j=j-(int)step){
				if(TimeResourceMap.get(j) != null) {
					TimeResourceMap.put(j, TimeResourceMap.get(j) - resource);
				}
				//log.info ("Update resource usage - {} interval, {}", j, TimeResourceMap.get(j));

			}

			for(int j = this.idx; j < timeMapSize; j=j+(int)step) {
				if(TimeResourceMap.get(j) != null) {
					TimeResourceMap.put(j, TimeResourceMap.get(j) - resource);
				}
				//log.info ("Update resource usage - {} interval, {}", j, TimeResourceMap.get(j));
			}

			task.endPreMonitoring();
			decreaseCounter(task);
			//removeJobMap(task);
			ExecTimeMap.remove(idx);

			//idx = null;

			this.pool.remove(task);
			//log.info("task.ID: {}", task.getID());

			if(SchedMap.containsKey(task.getID())){
				//log.info("Schedmap.get: {}", SchedMap.get(task.getID()).toString());
				SchedMap.get(task.getID()).cancel(false);
			}
			/*
			if(SchedMap.contains(task.getID())){
				log.info("Schedmap.get: {}", SchedMap.get(task.getID()).toString());
				SchedMap.get(task.getID()).cancel(false);
			}
			*/

			//this.schd.cancel(false);	// job thread removing

			task.setPoolID((long) 0.0);
			//printExecTimeMap();

			//log.info("[deSchedule] removed taskid: {} --- poolID {} idx {}", task.getID(), task.getPoolID(), idx);
		}

		public void setExecTime(Date date, Integer resource) {
			// delay nextExecution time
			//long newGCD = updateGCD(this.interval);
			//long lcm = gcd * this.interval / newGCD;
			int tempMax = 0;
			int maxIndex = 0;
			HashMap<Integer, Integer> maxUtil = new HashMap<Integer, Integer>();
			//gcd = newGCD;

			while(nextExecution < date.getTime())
				setNextExecution(nextExecution + intervalSize);
			
			// idx means a time slot (there must be #intervalSize of time slots)
			this.idx = (int)(nextExecution % timeMapSize);
			//this.idx = (int)(nextExecution % intervalSize);
			
			//log.info(">> nextExecutionTime: {}, idx: {}", nextExecution, idx);
			
			/* [dhjeon]
			 * if the time slot(ExecTimeMap) is empty, then use the time slot
			 * if not, find another empty time slot
			 * when all time slots are occupied, use a time slot which has the shortest array
			 */


			//check the TimeResourceMap Size
			if(timeMapSize < interval){
				timeMapSize = interval;
			}

			if(TimeResourceMap.get(idx) == null){
				TimeResourceMap.put(idx, resource);
			}else{
				int extra = 1;
				long step = interval / iterationNum;
			//	int step = 1;
				int minsize = TimeResourceMap.get(idx);

				int upperBound = this.idx + (int)interval % (int)timeMapSize;
				//int upperBound = this.idx / (int)intervalSize;
				for(int i = (int)step; i<upperBound;i=i+(int)step){
					int tmp = (this.idx+i) % (int)intervalSize;
					maxUtil.put(i, 0);

					for(int j=tmp-(int)step; j >= 0; j=j-(int)step){
						int jUtil = 0;
						if(TimeResourceMap.get(j) != null){
							jUtil = TimeResourceMap.get(j);
						}

						if(jUtil + resource > maxUtil.get(i)){
							maxUtil.put(i, jUtil + resource);
						}
					}

					for(int j = tmp; j < timeMapSize; j=j+(int)step){
						int jUtil = 0;
						if(TimeResourceMap.get(j) != null){
							jUtil = TimeResourceMap.get(j);
						}

						if(jUtil + resource > maxUtil.get(i)){
							maxUtil.put(i, jUtil + resource);
						}
					}

					if(maxUtil.get(i) > tempMax){
						maxIndex = i;
					}
				}

				// Update the resource utilization for every intervals
				this.idx = (this.idx + maxIndex) % (int)intervalSize;
				for(int j=this.idx-(int)step; j >= 0; j=j-(int)step){
					if(TimeResourceMap.get(j) == null){
						TimeResourceMap.put(j, resource);
					}else{
						TimeResourceMap.put(j, TimeResourceMap.get(j) + resource);
					}
				}

				for(int j = this.idx; j < timeMapSize; j=j+(int)step) {
					if (TimeResourceMap.get(j) == null) {
						TimeResourceMap.put(j, resource);
					} else {
						TimeResourceMap.put(j, TimeResourceMap.get(j) + resource);
					}
				}

				// Update next execution, idx
				this.setNextExecution(nextExecution + this.idx*timeSlotSize);
				this.idx = (int)(nextExecution % intervalSize);
				if(ExecTimeMap.get(idx) == null) {
					ExecTimeMap.put(idx, new ArrayList<Long>());
				}
				ExecTimeMap.get(idx).add(poolId);
				}

			/*if(ExecTimeMap.get(idx) == null) {
				ExecTimeMap.put(idx, new ArrayList<Long>());
				ExecTimeMap.get(idx).add(poolId);				
			} else {
				int extra = 1;
				int step = 5; // time(ms) to jump // default value: 1
				int minsize = ExecTimeMap.get(idx).size();
				
				for(int i=step; i<(int)intervalSize; i=i+step) {
					int tmp = (this.idx+i)%(int)intervalSize;
					
					// find another empty time slot
					if(ExecTimeMap.get(tmp) == null) {
						ExecTimeMap.put(tmp, new ArrayList<Long>());
						extra = i;
						break;
					} 
					// find a time slot with smaller size of array
					if(ExecTimeMap.get(tmp).size() < minsize) {
						minsize = ExecTimeMap.get(idx).size();
						extra = i;						
					}
				}*//*
				
				this.setNextExecution(nextExecution + extra);
				this.idx = (int)(nextExecution % intervalSize);
				ExecTimeMap.get(idx).add(poolId);
			}*/
			//log.info(">> final nextExecutionTime: {}, idx: {}", nextExecution, idx);
			//printExecTimeMap();
		}
		
		// print ExecTimeMap 
		public void printExecTimeMap() {
			for( Integer key : ExecTimeMap.keySet() ){
	            log.warn(">>> key: {}, val: {}", key, ExecTimeMap.get(key));
	        }
		}

		public long getPoolId(){
			return this.poolId;
		}

		public void setInterval(long interval){
			this.interval = interval;
		}

		public long getInterval(){
			return this.interval;
		}
		
		public long getNextExecution() {
			return this.nextExecution;
		}

		public void setNextExecution(long time){
			//check t is future time
			if(time > this.nextExecution)
				this.nextExecution = time;
		}
						
		// return counter of a physical switch
		public Integer getScheduledJobNumber(BasicMonitoringEntity task){
			PhysicalSwitch sw = task.getPhysicalSwtich();
			
			switch (task.getType()) {
			case SINGLE_FLOW_STAT:
				return singleFlowCounter.get(sw);
			case SINGLE_PORT_STAT:
				return singlePortCounter.get(sw);
			case AGG_FLOW_STAT:
				return aggFlowCounter.get(sw);
			case AGG_PORT_STAT:
				return aggPortCounter.get(sw);
			}
			return 0;
		}
		
		// each physical switch has array of tasks(jobs)
		public void addJobMap(BasicMonitoringEntity task){
			PhysicalSwitch sw = task.getPhysicalSwtich();
			switch (task.getType()) {
			case SINGLE_FLOW_STAT:
				if(singleFlowJobMap.get(sw) == null)
					singleFlowJobMap.put(sw, new ArrayList<SingleFlowStatMonitor>());
				singleFlowJobMap.get(sw).add((SingleFlowStatMonitor)task);
				break;
			case SINGLE_PORT_STAT:
				if(singlePortJobMap.get(sw) == null)
					singlePortJobMap.put(sw, new ArrayList<VirtualPortStatMonitor>());
				singlePortJobMap.get(sw).add((VirtualPortStatMonitor)task);
				break;
			case AGG_FLOW_STAT:
				if(FlowJobMap.get(sw) == null)
					FlowJobMap.put(sw, new ArrayList<FlowStatMonitor>());
				FlowJobMap.get(sw).add((FlowStatMonitor)task);
				break;
			case AGG_PORT_STAT:
				if(PortJobMap.get(sw) == null)
					PortJobMap.put(sw, new ArrayList<PortStatMonitor>());
				PortJobMap.get(sw).add((PortStatMonitor)task);
				break;
			}
		}

		// ? remove whole array?
		public void removeJobMap(BasicMonitoringEntity task){
			PhysicalSwitch sw = task.getPhysicalSwtich();
			switch (task.getType()) {
			case SINGLE_FLOW_STAT:
				singleFlowJobMap.get(task.getPhysicalSwtich()).remove(task);
				//singleFlowJobMap.remove(sw);
				break;
			case SINGLE_PORT_STAT:
				singlePortJobMap.remove(sw);
				break;
			case AGG_FLOW_STAT:
				FlowJobMap.remove(sw);
				break;
			case AGG_PORT_STAT:
				PortJobMap.remove(sw);
				break;
			}
		}
		
		// increase job counter of sw
		public void increaseCounter(BasicMonitoringEntity task){
			PhysicalSwitch sw = task.getPhysicalSwtich();
			switch (task.getType()) {
			case SINGLE_FLOW_STAT:
				if(singleFlowCounter.get(sw) == null) 
					singleFlowCounter.put(sw, 1);
				else 
					singleFlowCounter.put(sw, singleFlowCounter.get(sw) + 1);
				break;
			case SINGLE_PORT_STAT:
				if(singlePortCounter.get(sw) == null) 
					singlePortCounter.put(sw, 1);
				else 
					singlePortCounter.put(sw, singlePortCounter.get(sw) + 1);
				break;
			case AGG_FLOW_STAT:
				if(aggFlowCounter.get(sw) == null) 
					aggFlowCounter.put(sw, 1);
				else 
					aggFlowCounter.put(sw, aggFlowCounter.get(sw) + 1);
				break;
			case AGG_PORT_STAT:
				if(aggPortCounter.get(sw) == null) 
					aggPortCounter.put(sw, 1);
				else 
					aggPortCounter.put(sw, aggPortCounter.get(sw) + 1);
				break;
			}
		}
		
		// decrease job counter of sw
		public void decreaseCounter(BasicMonitoringEntity task){
			PhysicalSwitch sw = task.getPhysicalSwtich();
			switch (task.getType()) {
			case SINGLE_FLOW_STAT:
				if(singleFlowCounter.get(sw) != null) 
					singleFlowCounter.put(sw, singleFlowCounter.get(sw) - 1);
				break;
			case SINGLE_PORT_STAT:
				if(singlePortCounter.get(sw) != null) 
					singlePortCounter.put(sw, singlePortCounter.get(sw) - 1);
				break;
			case AGG_FLOW_STAT:
				if(aggFlowCounter.get(sw) != null) 
					aggFlowCounter.put(sw, aggFlowCounter.get(sw) - 1);
				break;
			case AGG_PORT_STAT:
				if(aggPortCounter.get(sw) != null) 
					aggPortCounter.put(sw, aggPortCounter.get(sw) - 1);
				break;
			}
		}
	}
	// ----- end of MonitoringPool
	
		
	// ?
	private class PoolManager implements Runnable{
		private MonitoringPool mp;
		public PoolManager(MonitoringPool mp){
			this.mp = mp;
		}
		@Override
		public void run() {
			// update Timer
			Date date = new Date();
			this.mp.setNextExecution(date.getTime() + this.mp.getInterval());
		}
	}

	/**
	 * Execute a function by the type of given task
	 * @param task
	 * @param mean
	 * @param dev
	 */
	public void addTask(long rid, BasicMonitoringEntity task, double mean, double dev) {
		switch (task.getType()) {
		case SINGLE_FLOW_STAT:
			log.info(">> SINGLE_FLOW_STAT: New task generation for flow stats gathering");
			try {
				this.addFlowTask(rid, (SingleFlowStatMonitor) task, mean, dev);
			} catch (IndexOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case SINGLE_PORT_STAT:
			log.info(">> VIRTUAL_PORT_STAT: New task generation for port stats gathering");
			try {
				this.addPortTask((VirtualPortStatMonitor) task, mean, dev); // [dhjeon]
			} catch (IndexOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case AGG_FLOW_STAT:
			
			break;
		case AGG_PORT_STAT:
			
			break;
		}
	}
	
	/**
	 * 
	 * @param task
	 * @param mean
	 * @param dev
	 * @throws IndexOutOfBoundException
	 */
	public void addFlowTask(long rid, SingleFlowStatMonitor task, double mean, double dev) throws IndexOutOfBoundException {

		//this.log.info("[addFlowTask] task ID : {}, {}", task.getPoolID(), task);
		
		/* remove task from pool when it already exists */
		if (task.getPoolID() != 0){
			// task has 0 of PoolID if it has not been assigned to the pool
			removeTask(task);
		}

		MonitoringPool targetPool = null;
		PhysicalSwitch sw = task.getPhysicalSwtich(); //get switch info

        long phyInterval = (long)((mean - this.k2 * dev) / this.intervalSize) * this.intervalSize;


		// find range [mean - k * dev, mean]
		long min = (long)((mean -  this.k2 * dev) / this.intervalSize) * this.intervalSize;
		long max = (long)((mean ) / this.intervalSize) * this.intervalSize;
		//long max = (long)((mean - this.k1 * dev) / this.intervalSize) * this.intervalSize;
		long optimalPoolIndex = 0;
		//log.info("min: {}, max: {}", (long)min, (long)max);
		//log.info("k1: {}, k2: {}, mean: {}, dev: {}", (double)k1, (double)k2, (double)mean, (double)dev);
		int jobnumber = 0, tempjob = 0;
		
		//this.log.info("[addFlowTask] values <mean {}> <dev {}> <min {}> <max {}>", mean, dev, min, max);
			
		/*
		 * find targetPool (between min and max) which has the highest number of jobs
		 * to send a monitoring request at once
		 * - job_number: the highest number of jobs in the targetPools between min and max
		 * - optimalPoolIndex: Index of the targetPool which has the highest number of jobs
		 */
		for(long i = min; i <= max; i += this.intervalSize){
		//for(long i = min; i <= max; i += this.intervalSize){
			//TODO : find targetPool
			if(poolMap.get(i)!=null){
				// the number of jobs assigned to this pool
				if(poolMap.get(i).getScheduledJobNumber(task)!=null){
					tempjob = poolMap.get(i).getScheduledJobNumber(task);
					//log.info("[Scheduler] # of jobs at slot {}: {}", i, tempjob);
				}

				if(tempjob > jobnumber){
					jobnumber = tempjob;
					optimalPoolIndex = i;
				}
			}
		}
		//log.info("Decided poolID: {}, the scheduled task #: {}", optimalPoolIndex, jobnumber);
				
		/*
		 * if there is no optimalPool, then assign new MonitoringPool
		 * Use mean as poolId
		 */

		if(task.getAggregatedScheduleStatus() == true){
			if(task.getScheduleStatus() == true){
				PhysicalNetwork.getScheduler().removeTask(task);
			}
			task.endPreMonitoring();
			return;
		}

		if(optimalPoolIndex == 0){
			//Long poolId = (long)(mean / this.intervalSize) * this.intervalSize;
			Long poolId = phyInterval;
			targetPool = new MonitoringPool(poolId, (long)(mean / this.intervalSize) * this.intervalSize);  // poolId and Interval - ?? same value


			poolMap.put(targetPool.getPoolId(), targetPool);


			//this.log.info("[addFlowTask] new pool >>>>> targetPool : {}", targetPool.getPoolId());
		}else {
			targetPool = poolMap.get(optimalPoolIndex); // assign optimalPool
			//this.log.info("[addFlowTask] optimal pool >>>>> targetPool : {}", targetPool.getPoolId());


			// merging
			this.log.info(" info -- merging -- job number : {}, entry number : {}", poolMap.get(optimalPoolIndex).getScheduledJobNumber(task), sw.getPhysicalFlowTable().getEntryNumber());
			if(((double)poolMap.get(optimalPoolIndex).getScheduledJobNumber(task) / (double)sw.getPhysicalFlowTable().getEntryNumber() )>= this.mergeThresh) {
				this.log.info("Now merging!!!");
				if(task.getPhysicalSwtich().getMergingLock()==false) {
					task.getPhysicalSwtich().lockMerging();
					poolMap.get(optimalPoolIndex).deScheduleForSwitch(task);        // delete the previously scheduled job from the pool
					poolMap.get(optimalPoolIndex).schedule(this.createAggregateTask(task, optimalPoolIndex));
					task.getPhysicalSwtich().freeMerging();
					return;
				}
			}


		}

/*
//		if (targetPool == null){
//			Integer poolId = poolCounter.getNewIndex();
//			targetPool = new MonitoringPool(poolId, (long)(mean / this.intervalSize) * this.intervalSize);
//			poolMap.put(targetPool.getPoolId(), targetPool);
//		}
*/

		targetPool.schedule(task);
	}
	
	// [dhjeon]
	public void addPortTask(VirtualPortStatMonitor task, double mean, double dev) throws IndexOutOfBoundException {
		this.log.info("[addPortTask] task ID : {}", task.getPoolID());
		
		if (task.getPoolID() != 0){
			// task has 0 of PoolID if it has not been assigned to the pool
			removeTask(task);
		}
		
		MonitoringPool targetPool = null;
		PhysicalSwitch sw = task.getPhysicalSwtich(); //get switch info 
		OVXSwitch vsw = task.getVirtualSwitch();
		long phyInterval = (long)((mean - this.k1 * dev) / this.intervalSize) * this.intervalSize;

		// find range [mean - k * dev, mean]
		long min = (long)((mean - this.k2 * dev) / this.intervalSize) * this.intervalSize;
		long max = (long)((mean ) / this.intervalSize) * this.intervalSize;
		//long max = (long)((mean + this.k1 * dev) / this.intervalSize) * this.intervalSize;
		long optimalPoolIndex = 0;
		int jobnumber = 0, tempjob = 0;
		
		/* Find targetPool (between min and max) which has the highest number of jobs
		 * to send a monitoring request at once
		 * - jobnumber: the highest number of jobs in the targetPools between min and max
		 * - optimalPoolIndex: Index of the targetPool which has the highest number of jobs
		 */
		for(long i = min; i <= max; i += this.intervalSize){
			//TODO : find targetPool
			if(poolMap.get(i)!=null){
				// the number of jobs assigned to this pool
				if(poolMap.get(i).getScheduledJobNumber(task)!=null) // [dhjeon]
					tempjob = poolMap.get(i).getScheduledJobNumber(task);
				if(tempjob > jobnumber){
					jobnumber = tempjob;
					optimalPoolIndex = i;
				}
			}
		}
		
		this.log.info("[addPortTask] values <tempjob {}> <jobnumber {}> <optimalPoolIndex {}>", tempjob, jobnumber, optimalPoolIndex);
		
		/*
		 * if there is no optimalPool, then assign new MonitoringPool
		 * Use mean as poolId
		 */
		if(optimalPoolIndex == 0){
			//Long poolId = (long)(mean / this.intervalSize) * this.intervalSize;
			Long poolId = phyInterval;
			targetPool = new MonitoringPool(poolId, (long)(mean / this.intervalSize) * this.intervalSize);  // poolId and Interval - ?? same value
			poolMap.put(targetPool.getPoolId(), targetPool);
			//this.log.info("[addPortTask] new pool >>>>> targetPool : {}", targetPool.getPoolId());
		}else {
			targetPool = poolMap.get(optimalPoolIndex); // assign optimalPool
			//this.log.info("[addPortTask] optimal pool >>>>> targetPool : {}", targetPool.getPoolId());

			/*
			this.log.info(" info -- merging -- job number : {}, entry number : {}", poolMap.get(optimalPoolIndex).getScheduledJobNumber(task), sw.getPhysicalFlowTable().getEntryNumber());
			if(((double)poolMap.get(optimalPoolIndex).getScheduledJobNumber(task) / (double)vsw.getPorts().size() >= this.mergeThresh)){
				this.log.info("Port task - now merging!!!");


				if(task.getPhysicalSwtich().getMergingLock()==false) {
					task.getPhysicalSwtich().lockMerging();
					poolMap.get(optimalPoolIndex).deScheduleForSwitch(task);        // delete the previously scheduled job from the pool
					poolMap.get(optimalPoolIndex).schedule(this.createAggregateTask(task, optimalPoolIndex));
					task.getPhysicalSwtich().freeMerging();
					return;
				}

			}
			*/

			// merging
			/*
			if((poolMap.get(optimalPoolIndex).getScheduledJobNumber(task) / sw.getPhysicalFlowTable().getEntryNumber() )>= this.mergeThresh) {
				poolMap.get(optimalPoolIndex).deScheduleForSwitch(task);		// delete the previously scheduled job from the pool
				poolMap.get(optimalPoolIndex).schedule(this.createAggregateTask(task, optimalPoolIndex));
			}		
			*/	
		}

/*
//		if (targetPool == null){
//			Integer poolId = poolCounter.getNewIndex();
//			targetPool = new MonitoringPool(poolId, (long)(mean / this.intervalSize) * this.intervalSize);
//			poolMap.put(targetPool.getPoolId(), targetPool);
//		}
*/
		targetPool.schedule(task);
	
	}
	
	
	public void removeTask(BasicMonitoringEntity task){
		poolMap.get(task.getPoolID()).deSchedule(task);
	}
	
	public void removeSwitch(PhysicalSwitch sw){
		//remove all tasks associated with PhysicalSwitch sw
	}
	
	public void addFlowAggJob(PhysicalSwitch sw){
		
	}
	
	// ?
	private BasicMonitoringEntity createAggregateTask(BasicMonitoringEntity task, Long Index){
		this.log.info("=============== Aggregation started!!!!!!");
		PhysicalSwitch sw = task.getPhysicalSwtich();
		switch(task.getType()){
		case SINGLE_FLOW_STAT:
			//FlowStatMonitor newFlowJob = new FlowStatMonitor(sw);
			FlowStatMonitor newFlowJob = sw.getAggregatedFlowTask();
			//newFlowJob.run();
			//log.info("testlog: {}", newFlowJob.UpdateAndGetJobSize());
			//FlowStatMonitor newFlowJob = new FlowStatMonitor(sw);
			this.aggFlowMap.put(sw, Index);
			return newFlowJob;
		case SINGLE_PORT_STAT:
			PortStatMonitor newPortJob = new PortStatMonitor(sw);
			this.aggPortMap.put(sw, Index);
			return newPortJob;
		default:
			return null;
		}
	}

	/*private long updateGCD(long newValue){
		long a = this.gcd;
		while (newValue != 0) {
			long r = a % newValue;
			a = newValue;
			newValue = r;
		}
		return a;
	}*/


}
