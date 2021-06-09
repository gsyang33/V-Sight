package net.onrc.openvirtex.elements.datapath.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.onrc.openvirtex.elements.network.PhysicalNetwork;
import net.onrc.openvirtex.exceptions.IndexOutOfBoundException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StatInterval {
    Logger log = LogManager.getLogger(StatInterval.class.getName());

	private ConcurrentHashMap<Long, MeanDev> meanDevMap;
	private ConcurrentHashMap<Long, Long> prevTime;
	private ConcurrentHashMap<Long, List<Double>> intervals; // interval => sec
	private Integer sampleSize;
	private double hitThreshold;
	private Integer rescheduleSampleSize;
	private int rescheduleIterCount;
	
	private MeanDev meanDev;
	public class MeanDev{
		private Double mean, dev;
		public MeanDev(Double mean, Double dev){
			this.mean = mean;
			this.dev = dev;
		}
		public void setMeandev(Double mean, Double dev){
			this.mean = mean;
			this.dev = dev;
		}
		public Double getMean(){
			return this.mean;
		}
		public Double getDev(){
			return this.dev;
		}
	}
	
    /**
     * generate StatInterval with default sampleSize
     */
	public StatInterval(){
		this.meanDevMap = new ConcurrentHashMap<>();
		this.prevTime = new ConcurrentHashMap<Long, Long>();
		this.intervals = new ConcurrentHashMap<Long, List<Double>>();
		this.sampleSize = 40;
		this.hitThreshold = 0.7;
		this.meanDev = new MeanDev(null, null);
		this.rescheduleIterCount = 0;
	}
	
    /**
     * generate StatInterval with defined sampleSize
     *
     * @param sampleSize - minimum sample size
     */
	public StatInterval(Integer sampleSize){
		this.meanDevMap = new ConcurrentHashMap<>();
		this.prevTime = new ConcurrentHashMap<Long, Long>();
		this.intervals = new ConcurrentHashMap<Long, List<Double>>();
		this.sampleSize = sampleSize;
		this.meanDev = new MeanDev(null, null);
	}
	
	/**
	 * 
	 * @param bme
	 * @param rid		- tenant ID
	 * @param interval  - interval to add
	 */
	public void addInterval(BasicMonitoringEntity bme, long rid, long interval){
		if(!intervals.containsKey(rid)){
			intervals.put(rid, new ArrayList<Double>());
			intervals.get(rid).add(new Double(interval));
			return;
		}
		intervals.get(rid).add(new Double(interval));
		
		/* 
		 * when the number of intervals is over sampleSize
		 * calculate mean and dev of the intervals
		 */

		if(this.getIntervals(rid).size() > this.sampleSize){
			this.calculateMeanDev(rid);

			MeanDev tempMeanDev = getMinMeanDev(); // get minimum mean
			//this.log.info("[addInterval] Mean {} temp {}", meanDev.getMean(), tempMeanDev.getMean());
			this.resetIntervals(rid);

			// current mean value should be minimum
			if(meanDev.getMean() == null || tempMeanDev.getMean() < meanDev.getMean()) {
				this.meanDev = tempMeanDev;
				this.meanDevMap.put(rid, this.meanDev);
				//this.log.info("[addInterval] check schedule status: {}, hitrate: {}", bme.getScheduleStatus(), bme.getHitrate());

				//log.info("[StatInterval] AggregatedScheduleStatus: {}", bme.getAggregatedScheduleStatus());

				if (!bme.getAggregatedScheduleStatus()) {
					if (bme.getScheduleStatus() == false ) {
						//this.log.info("[addInterval] New value!!! mean {}, dev {}", this.meanDev.getMean(), this.meanDev.getDev());
						PhysicalNetwork.getScheduler().addTask(rid, bme, meanDev.getMean(), meanDev.getDev()); // Handing bme to the scheduler
					}else if(bme.getScheduleStatus() == true ){
						if(bme.getHitrate() < hitThreshold && rescheduleIterCount >= this.sampleSize){
							this.log.info("Reschedule the task {} due to the low hit rate", bme.taskID);
							this.rescheduleIterCount = 0;
							// [ksyang] task remove implementation required.
							PhysicalNetwork.getScheduler().addTask(rid, bme, meanDev.getMean(), meanDev.getDev()); // Handing bme to the scheduler
						}else{
							this.rescheduleIterCount++;
						}
					}
				}

				if(bme.getAggregatedScheduleStatus() && bme.getScheduleStatus()){
				    PhysicalNetwork.getScheduler().removeTask(bme);
                }
			}
		}
	}
	
	/**
	 * calculate mean and dev of intervals for given rid
	 * 
	 * @param rid
	 */

	// ksyang --> removed synchronized
	public void calculateMeanDev(long rid){
//		if (!this.meanDevs.containsKey(rid)) { 
//			double defaultInterval = (double)OpenVirteXController.getInstance().getFlowStatsRefresh() * 1000;
//			this.meanDevs.put(rid, new MeanDev(defaultInterval, 0.0));
//			return;
//		}

		List<Double> intervalList = getIntervals(rid); 

		//extract subset of sample size
		List<Double> subList = intervalList.subList(intervalList.size() - sampleSize, intervalList.size());
	
		Double sum = 0.0, mean = 0.0, devSum = 0.0, dev = 0.0;
		for(double d : subList)
			sum += d;
		mean = sum / subList.size();
		for(double d : subList)
			devSum += Math.pow(d - mean, 2);
		dev = Math.sqrt(devSum / subList.size());

		this.meanDevMap.put(rid, new MeanDev(mean, dev));
	}
	
	public void setSampleSize(Integer sampleSize, long rid){
		this.sampleSize = sampleSize;
		//calculateMeanDev(rid);
	}
	
    /**
     * return a MeanDev with corresponding rid
     *
     * @return meanDev value 
     */
	public MeanDev getMeanDev(long rid) {
		if (!this.meanDevMap.containsKey(rid)) return null;
		else return this.meanDevMap.get(rid);
	};
	
    /**
     * @return minimum mean value and its dev
     */
	public MeanDev getMinMeanDev(){
		//MeanDev min = new MeanDev(Double.MAX_VALUE, 0);
		MeanDev min = new MeanDev(0.0, 0.0);
		
		for(Map.Entry<Long, MeanDev> elem : meanDevMap.entrySet()){
			if (min.getMean() <= 0.0)
				min = elem.getValue();
			if (min.getMean() > elem.getValue().getMean()) {
				min = elem.getValue();
				log.info("** changed");
			}
		}
		
		return min;
	}

	public void setMinMeanDev(long rid, double mean, double dev){
		MeanDev phyDistfromScheduler = new MeanDev(mean, dev);
		if(meanDevMap.containsKey(rid)){
			meanDevMap.remove(rid);
			meanDevMap.put(rid, phyDistfromScheduler);
		}
	}
	
	public void setPrevTime(long rid, long time){
		this.prevTime.put(rid, time);
	}
	
	public long getPrevTime(long rid){
		return this.prevTime.get(rid);
	}

	public boolean containsPrevTime(long rid){
		return this.prevTime.containsKey(rid);
	}

	public List<Double> getIntervals(long rid){
		return this.intervals.get(rid);
	}

	public void resetIntervals(long rid){
	    int window = this.sampleSize / 3;
	    for (int i=0;i<window;i++){
	        this.intervals.get(rid).remove(i);
        }
    }
}
