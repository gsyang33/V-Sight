package net.onrc.openvirtex.elements.datapath;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import net.onrc.openvirtex.elements.port.OVXPort;
import net.onrc.openvirtex.exceptions.SwitchMappingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.onrc.openvirtex.core.OpenVirteXController;
import net.onrc.openvirtex.core.io.OVXSendMsg;
import net.onrc.openvirtex.elements.datapath.statistics.BasicMonitoringEntity;
import net.onrc.openvirtex.elements.datapath.statistics.SingleFlowStatMonitor;
import net.onrc.openvirtex.elements.datapath.statistics.StatInterval;
import net.onrc.openvirtex.elements.datapath.statistics.StatInterval.MeanDev;
import net.onrc.openvirtex.elements.network.PhysicalNetwork;
import net.onrc.openvirtex.exceptions.IndexOutOfBoundException;
import net.onrc.openvirtex.messages.OVXFlowMod;
import net.onrc.openvirtex.messages.OVXMessage;
import net.onrc.openvirtex.messages.OVXStatisticsRequest;

import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowStatsEntry;
import org.projectfloodlight.openflow.protocol.OFFlowStatsRequest;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.types.TableId;


public class PhysicalFlowEntry {
	Logger log = LogManager.getLogger(PhysicalFlowEntry.class.getName());
	
	private PhysicalSwitch psw;
	private OVXFlowMod ovxFlowmod;
	private boolean cache;
	private boolean lock;
    private boolean edge_lock; // [ksyang] - fr aggregate
    private long flowStatsTime;
	private long lastReqTime;	 // for checking the request is first or not
	private OFFactory ofFactory;
	private StatInterval statInterval;
	private BasicMonitoringEntity flowMonitor;
	private OFFlowStatsEntry flowStats;
    private OFFlowStatsEntry edgeFlowStats;
	private int totalRequest;
	private int hitCount;
	private int missCount;
	private int oldCount;
	private int init_pass;
	private double hitrate;
	private int accuracyxid;
	private Long edgeswitchID;
	private Long edgeEntryCookie;
	private boolean aggregateMonitoring;
	
	public PhysicalFlowEntry (OVXFlowMod flowmod, PhysicalSwitch ps){
		this.psw = ps;
		this.ovxFlowmod = flowmod;
		this.cache = false;
		this.lock = false;
		this.edge_lock = false;
		this.flowStatsTime = 0;
		this.lastReqTime = 0;
		this.ofFactory = OFFactories.getFactory(ps.getOfVersion());
		this.statInterval = new StatInterval();
		this.flowMonitor = new SingleFlowStatMonitor(ps, flowmod, this);
		this.totalRequest = 0;
		this.hitCount = 0;
		this.missCount= 0;
		this.oldCount = 0;
		this.hitrate = -1;
		this.init_pass = 0;
		this.accuracyxid = 10;
		this.aggregateMonitoring = false;

	}

	public OVXFlowMod getFlowMod(){
		return this.ovxFlowmod;
	}

	public void setFlowStatistics(OFFlowStatsEntry stats){
		this.flowStats = stats;
		this.flowStatsTime = new Date().getTime();
	}

    public void setEdgeFlowStatistics(OFFlowStatsEntry stats){
        this.edgeFlowStats = stats;
    }
	
	public synchronized void setLock() {
		this.lock = true;
		return;
	}


    public synchronized void setEdgeLock() {
        this.edge_lock = true;
        return;
    }


    public OFFlowStatsEntry getEdgeFlowStatistics(){
        //long now = new Date().getTime(); // current request time
        //this.increaseTotalRequest();



        OFFlowStatsRequest ofFlowStatsRequest = this.ofFactory.buildFlowStatsRequest()
                .setXid(565)
                .setMatch(this.ovxFlowmod.getFlowMod().getMatch())
                .setOutPort(this.ovxFlowmod.getFlowMod().getOutPort())
                .setTableId(TableId.ALL)
                .build();
        OVXStatisticsRequest req = new OVXStatisticsRequest(ofFlowStatsRequest);
        psw.sendMsg(req, psw);

        while(!edge_lock) {
            try {
                Thread.sleep(100); }
            catch(InterruptedException ex) {
                Thread.currentThread().interrupt(); }
            // do nothing. just wait for a reply
            // flowStats updated : OVXFlowStatsReplay -> setFlowStatistics(stats);
        }
        edge_lock = false;
        // wait until a reply arrives
//
//
//        // Update the interval
//        if(this.lastReqTime != 0) { // skip this. when it is the first request
//            //this.log.info("New flowstat time add - rid: {}, now: {}, lastreqTIme: {}", rid, now, lastReqTime);
//            this.statInterval.addInterval(this.flowMonitor, rid, (now - lastReqTime)); // lastReqTime
//        }
//
//        this.statInterval.setPrevTime(rid, now);
//        this.lastReqTime = now;

        return this.edgeFlowStats;
    }



	public OFFlowStatsEntry getFlowStatistics(long rid){
		long now = new Date().getTime(); // current request time
		this.increaseTotalRequest();


		if(this.init_pass < 15){

			OFFlowStatsRequest ofFlowStatsRequest = this.ofFactory.buildFlowStatsRequest()
					.setXid(rid)
					.setMatch(this.ovxFlowmod.getFlowMod().getMatch())
					.setOutPort(this.ovxFlowmod.getFlowMod().getOutPort())
					.setTableId(TableId.ALL)
					.build();
			OVXStatisticsRequest req = new OVXStatisticsRequest(ofFlowStatsRequest);
			psw.sendMsg(req, psw);

			// wait until a reply arrives

			//log.info("init_pass: {}", this.init_pass);
			while(!lock) {
				try {
					Thread.sleep(100); }
				catch(InterruptedException ex) {
					Thread.currentThread().interrupt(); }
				// do nothing. just wait for a reply
				// flowStats updated : OVXFlowStatsReplay -> setFlowStatistics(stats);
			}
			lock = false;
			this.increaseInitPass();
			return this.flowStats;
		}




		/******* if cached *******/
		if(this.cache  || this.psw.getAggregatedFlowMonitoringStatus()) {
			// Find cache

			long prevTime;
			if(this.psw.getAggregatedFlowMonitoringStatus() && this.statInterval.containsPrevTime(rid) == false){
				prevTime = 0;
			}else{
				prevTime = this.statInterval.getPrevTime(rid);
			}

			MeanDev ridMeanDev = this.statInterval.getMeanDev(rid);
			boolean criteria = false;
			//if(ridMeanDev != null){
			//	ridMean= ridMeanDev.getMean();
			//}
			//log.info("ridMean: {}", ridMean);

			//log.info("flowStatsTime: {}, getPrevTime: {}, now: {}", flowStatsTime, this.statInterval.getPrevTime(rid), now);
			if(ridMeanDev != null){
			    //log.info("calculation: mean: {}, now: {}, flowStatsTime: {}", now, flowStatsTime, ridMeanDev.getMean());
			    criteria = (now - flowStatsTime < ridMeanDev.getMean());
            }else{
                criteria = flowStatsTime >= prevTime;
            }
            //log.info("flow - criteria: {}", criteria);
            if(criteria){

				//Update the interval
                this.increaseHitCount();
                this.statInterval.addInterval(this.flowMonitor, rid, (now - lastReqTime));
				this.statInterval.setPrevTime(rid, now);
				this.lastReqTime = now;

				if(this.totalRequest > 100 && this.totalRequest < 120){
					this.log.info("Disaggregated accuracy flow time {} {} {} - cached time: {}, current Time: {}, difference: {}",this.psw.getName(), this.getFlowMod().getFlowMod().getCookie().toString(),this.psw.getAggregatedFlowMonitoringStatus(), flowStatsTime, now, now - flowStatsTime);
					this.log.info("Disaggregated accuracy flow V-Sight data cookie: {} xid: {}, byte: {} packet: {}", this.ovxFlowmod.getFlowMod().getCookie(), this.accuracyxid, this.flowStats.getByteCount().getValue(), this.flowStats.getPacketCount().getValue());
					OFFlowStatsRequest ofFlowStatsRequest = this.ofFactory.buildFlowStatsRequest()
							.setXid(this.accuracyxid)
							.setMatch(this.ovxFlowmod.getFlowMod().getMatch())
							.setOutPort(this.ovxFlowmod.getFlowMod().getOutPort())
							.setTableId(TableId.ALL)
							.build();
					OVXStatisticsRequest req = new OVXStatisticsRequest(ofFlowStatsRequest);
					psw.sendMsg(req, psw);
					this.accuracyxid++;
				}
				return this.flowStats;	//return cache value
			}
			this.log.info("now old: {}, {}, {}", flowStatsTime, prevTime, flowStatsTime - prevTime);
			this.increaseOldCount();
		}else{
			this.increaseMissCount();
		}
		
		/******* when this is the first request or not cached *******/
		/******* directly toss the msg *******/
		// Here -- No pre-monitoring, and not the first request. 
		// Two - cases. Not suitable for pre-monitoring or stacking some intervals.
		
		// First, send the request message. Then, wait until the message arrives. 
		// send a request msg to psw
		OFFlowStatsRequest ofFlowStatsRequest = this.ofFactory.buildFlowStatsRequest()
                .setXid(888)
                .setMatch(this.ovxFlowmod.getFlowMod().getMatch())
                .setOutPort(this.ovxFlowmod.getFlowMod().getOutPort())
                .setTableId(TableId.ALL)
                .build();
        OVXStatisticsRequest req = new OVXStatisticsRequest(ofFlowStatsRequest);
        psw.sendMsg(req, psw);
        
        // wait until a reply arrives

        while(!lock) {
        	try {
        	    Thread.sleep(100); }
        	catch(InterruptedException ex) {
        	    Thread.currentThread().interrupt(); }
        	// do nothing. just wait for a reply
        	// flowStats updated : OVXFlowStatsReplay -> setFlowStatistics(stats);
        }
        lock = false;
      
		// Update the interval
        if(this.lastReqTime != 0) { // skip this. when it is the first request
            //this.log.info("New flowstat time add - rid: {}, now: {}, lastreqTIme: {}", rid, now, lastReqTime);
        	this.statInterval.addInterval(this.flowMonitor, rid, (now - lastReqTime)); // lastReqTime
		}

		this.statInterval.setPrevTime(rid, now);
		this.lastReqTime = now;
		
		return this.flowStats;
	}

	public StatInterval getStatInterval(){
		return this.statInterval;
	}
	
	public synchronized void startCache(){
		this.cache = true;
	}
	
	public synchronized void stopCache(){
		this.cache = false;
		//log.info("set the status of the task - flow entry: {}, the value: {}", this.getFlowMod().getFlowMod().getCookie().toString(), this.cache);

	}


	public boolean isPremonitored(){
		return this.cache;
	}

	public double getHitrate(){
		this.hitrate = (double)this.hitCount / (double)(this.totalRequest-40-15);
		return hitrate;
//		return -1;
	}


	public synchronized void increaseTotalRequest(){
		this.totalRequest++;
		//log.info("[COUNT] Total: {}", this.totalRequest);
	}

	public synchronized void increaseHitCount(){
		this.hitCount++;
		//log.info("[COUNT-HIT] - {}, {}", this.psw.getSwitchName(), this.getFlowMod().getFlowMod().getCookie().toString());
		log.info("[FLOW] [Hit] Switch Aggregated {}, Pswitch {} Flow {}, Total {}, Hit {}, Rate {}", this.psw.getAggregatedFlowMonitoringStatus(), this.psw.getName(), this.getFlowMod().getFlowMod().getCookie().toString(), this.totalRequest-this.init_pass, this.hitCount, (double)this.hitCount / (this.totalRequest-40-this.init_pass));

	}

	public synchronized void increaseMissCount(){
		this.missCount++;
		//log.info("[COUNT-MISS] - {}, {}", this.psw.getSwitchName(), this.getFlowMod().getFlowMod().getCookie().toString());
		//log.info("[COUNT-MISS] Total: {}, Miss: {} - Rate: {}", this.totalRequest, this.missCount, (double)this.missCount / (this.totalRequest-30));
		log.info("[FLOW] [Miss] Switch Aggregated {}, Pswitch {} Flow {}, Total {}, Miss {}, Rate {}", this.psw.getAggregatedFlowMonitoringStatus(),  this.psw.getName(), this.getFlowMod().getFlowMod().getCookie().toString(), this.totalRequest-this.init_pass, this.missCount, (double)this.missCount / (this.totalRequest-40-this.init_pass));

	}

	public synchronized void increaseOldCount(){
		this.oldCount++;
		//log.info("[COUNT-OLD] Total: {}, Old: {} - Rate: {}", this.totalRequest, this.oldCount, (double)this.oldCount / (this.totalRequest-30));
		log.info("[FLOW] [Old] Switch Aggregated {}, Pswitch {} Flow {}, Total {}, Old {}, Rate {}", this.psw.getAggregatedFlowMonitoringStatus(),  this.psw.getName(), this.getFlowMod().getFlowMod().getCookie().toString(), this.totalRequest-this.init_pass, this.oldCount, (double)this.oldCount / (this.totalRequest-40-this.init_pass));

	}

	public void increaseInitPass(){
		this.init_pass++;
	}

	public BasicMonitoringEntity getPremonitor(){
		return this.flowMonitor;
	}
//	public boolean hasStatistics(long xid){
//		if(flowStats.containsKey(xid)){
//			return true;
//		}
//		return false;
//	}

	public void setAggregateMonitoring() {
		this.aggregateMonitoring = true;
	}

	public void offAggregateMonitoring() {
		this.aggregateMonitoring = false;
	}

	public synchronized Long getEdgeswitchID(){
	    return edgeswitchID;
    }

    public synchronized Long getEdgeEntryCookie(){
        return edgeEntryCookie;
    }

    public synchronized void setEdgeswitchID(Long id){
	    this.edgeswitchID = id;
    }

    public synchronized void setEdgeEntryCookie(Long cookie){
        this.edgeEntryCookie = cookie;
    }


}
