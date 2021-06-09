package net.onrc.openvirtex.elements.port;
// created by dahyun jeon

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.onrc.openvirtex.api.service.handlers.TenantHandler;
import net.onrc.openvirtex.core.OpenVirteXController;
import net.onrc.openvirtex.core.io.OVXSendMsg;
import net.onrc.openvirtex.db.DBManager;
import net.onrc.openvirtex.elements.datapath.PhysicalFlowEntry;
import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.elements.datapath.statistics.BasicMonitoringEntity;
import net.onrc.openvirtex.elements.datapath.statistics.SingleFlowStatMonitor;
import net.onrc.openvirtex.elements.datapath.statistics.SinglePortStatMonitor;
import net.onrc.openvirtex.elements.datapath.statistics.StatInterval;
import net.onrc.openvirtex.elements.datapath.statistics.StatInterval.MeanDev;
import net.onrc.openvirtex.elements.link.PhysicalLink;
import net.onrc.openvirtex.elements.network.PhysicalNetwork;
import net.onrc.openvirtex.exceptions.IndexOutOfBoundException;
import net.onrc.openvirtex.messages.OVXFlowMod;
import net.onrc.openvirtex.messages.OVXPortStatus;
import net.onrc.openvirtex.messages.OVXStatisticsRequest;
//import net.onrc.openvirtex.messages.statistics.OVXPortStatisticsRequest;
import net.onrc.openvirtex.messages.statistics.OVXPortStatsRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.OFStatsType;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowStatsEntry;
import org.projectfloodlight.openflow.protocol.OFFlowStatsRequest;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFPortStatsEntry;
import org.projectfloodlight.openflow.protocol.OFPortStatsRequest;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;

public class PhysicalPortEntry {
	Logger log = LogManager.getLogger(PhysicalPortEntry.class.getName());
	
	//private OFPortStatsEntry portStats;
	//private ConcurrentHashMap<Long, OFPortStatsEntry> portStats; // original
	private ConcurrentHashMap<Short, OFPortStatsEntry> portStats; // [dhjeon]
    private OFFactory ofFactory; // [dhjeon]
	private long lastReqTime ;
	private StatInterval statInterval;
	private BasicMonitoringEntity portMonitor;
	private boolean lock; // [dhjeon]
	//private boolean cache;
	private PhysicalSwitch psw;
	
	public PhysicalPortEntry (PhysicalSwitch sw, short port){
		this.psw = sw; // [dhjeon]
		this.lock = false;		
		this.lastReqTime = 0;
		this.ofFactory = OFFactories.getFactory(sw.getOfVersion()); // [dhjeon]
		this.statInterval = new StatInterval();
		this.portMonitor = new SinglePortStatMonitor(sw, port);
		//this.portStats = new OFPortStatsEntry(); // [dhjeon]
		this.portStats = new ConcurrentHashMap<Short, OFPortStatsEntry>();
		
//		try {
//		MeanDev md = getStatInterval().getMeanDev(); 
//		double defaultInterval = (double)OpenVirteXController.getInstance().getFlowStatsRefresh() * 1000;
//		PhysicalNetwork.getScheduler().addTask(portMonitor, defaultInterval, 0.0);
//	} catch (IndexOutOfBoundException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
	}
	
	public void setPortStatistics(Short portnum, OFPortStatsEntry stats){ // long xid, OFPortStatsEntry stats){ // [dhjeon]
		//this.log.info("[setFlowStatistics] PORT stats : {}", stats.toString());
		//this.portStats = stats;
		this.portStats.put(portnum, stats);
	}
	
	public void setLock() {
		this.lock = true;
		return;
	}

	public OFPortStatsEntry getPortStatistics(long xid, short portnum){
		long now = new Date().getTime(); // current request time
		
		/* [dhjeon] need cache???
		if(this.cache == true) {
			// Find cache
			if(flowStatsTime > this.statInterval.getPrevTime(rid) && flowStatsTime < now) {
				//Update the interval
				this.statInterval.addInterval(this.flowMonitor, rid, (now - lastReqTime));
				this.statInterval.setPrevTime(rid, now);
				this.lastReqTime = now;
				
				this.log.info("[getFlowStatistics] CACHED flow stats");
				
				return this.flowStats;	//return cache value
			}
		}
		*/

		OFPortStatsRequest ofPortStatsRequest = this.ofFactory.buildPortStatsRequest()
        		//.setXid(xid) // [dhjeon] need xid?
                .setPortNo(OFPort.ANY) //OFPort.ofShort(portnum)) 
                .build();
		
        OVXStatisticsRequest req = new OVXStatisticsRequest(ofPortStatsRequest);
        psw.sendMsg(req, psw); //(OVXSendMsg)this);

        // wait until a reply arrives
        while(!lock) {
        	try {
        	    Thread.sleep(100); }
        	catch(InterruptedException ex) {
        	    Thread.currentThread().interrupt(); }
        	// do nothing. just wait for a reply
        }
        lock = false;
      
		// Update the interval
        if(this.lastReqTime != 0) { // skip this. when it is the first request
        	this.statInterval.addInterval(this.portMonitor, xid, (now - lastReqTime)); // lastReqTime
		}

		this.statInterval.setPrevTime(xid, now);
		this.lastReqTime = now;
		
		return this.portStats.get(portnum); // .get(xid);
	}
	// [dhjeon]
	
	public StatInterval getStatInterval(){
		return this.statInterval;
	}
	
	/*
	public void startStatCache(){
		this.cache = true;
	}
	
	public void stopStatCache(){
		this.cache = false;
	}
	*/
	
	public boolean hasStatistics(Short portnum){
		if(portStats.containsKey(portnum)){
			return true;
		}
		return false;
	}
}
