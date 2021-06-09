package net.onrc.openvirtex.elements.datapath.statistics;

import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;

public abstract class BasicMonitoringEntity implements Runnable{

	public enum EntityType {SINGLE_FLOW_STAT, SINGLE_PORT_STAT, AGG_FLOW_STAT, AGG_PORT_STAT};
	
	protected final EntityType type;
	protected long poolID;
	protected long taskID;
	protected int jobSize;
	private boolean cache;

	private PhysicalSwitch sw;
	public BasicMonitoringEntity(EntityType e) {

		this.type = e;
		this.taskID = System.identityHashCode(this);
	}
	/**
     * @return the switch which a BME belongs
     */
	public PhysicalSwitch getPhysicalSwtich(){
		return this.sw;
	}
	public void setPoolID(Long id){
		this.poolID = id;
	}
    /**
     * @return the Pool which a BME scheduled
     */
	public Long getPoolID(){
		return this.poolID;
	}

	/**
	 * changes the pre-monitoring status of the element
	 */
	public void startPreMonitoring(){
		
	}

	/**
	 * changes the pre-monitoring status of the element
	 */
	public void endPreMonitoring(){

	}

	/**
	 * Update current job size for each pre-monitor
	 */
	public int UpdateAndGetJobSize(){
		return jobSize;
	}

	/**
	 * Get previous (before updating) job size for each pre-monitor
	 */
	public int GetJobSize(){
		return jobSize;
	}

	
	public EntityType getType(){
		return this.type;
	}

	public boolean getScheduleStatus(){
		return false;
	}

	public boolean getAggregatedScheduleStatus(){
		return false;
	}
	public double getHitrate(){
		return 0;
	}

	public long getID(){
		return taskID;
	}


}
