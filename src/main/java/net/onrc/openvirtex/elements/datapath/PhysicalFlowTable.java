package net.onrc.openvirtex.elements.datapath;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import net.onrc.openvirtex.elements.port.OVXPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.OFFlowModCommand;

import net.onrc.openvirtex.elements.datapath.statistics.BasicMonitoringEntity;
import net.onrc.openvirtex.elements.datapath.statistics.FlowStatMonitor;
import net.onrc.openvirtex.exceptions.MappingException;
import net.onrc.openvirtex.messages.OVXFlowMod;
import org.projectfloodlight.openflow.protocol.match.MatchField;

public class PhysicalFlowTable {
	private final Logger log = LogManager.getLogger(PhysicalFlowTable.class.getName());
	private PhysicalSwitch psw;
	private ConcurrentHashMap<Long, PhysicalFlowEntry> entryMap;
	protected ConcurrentHashMap<Long, OVXPort> pcookieInPortMap;
	protected ConcurrentHashMap<Long, OVXPort> pcookieOutPortMap;
	private int number;
	private BasicMonitoringEntity tableMonitor;
	
	public PhysicalFlowTable (PhysicalSwitch psw){
		this.psw = psw;
		this.entryMap = new ConcurrentHashMap<Long, PhysicalFlowEntry>();
		//this.tableMonitor = new FlowStatMonitor(psw);
		this.pcookieInPortMap = new ConcurrentHashMap<Long, OVXPort>();
		this.pcookieOutPortMap = new ConcurrentHashMap<Long, OVXPort>();
		this.number = 0;
	}
	
	public int getEntryNumber(){
		return this.entryMap.size();
		//		this.number;
	}
	
	public boolean handleFlowMods(OVXFlowMod fm, long vCookie) {
		switch(fm.getFlowMod().getCommand()){
		case ADD:
			//boolean success = doFlowModAdd(fm);
			return doFlowModAdd(fm);
		case MODIFY:
		case MODIFY_STRICT:
			return doFlowModModify(fm);
		case DELETE:
			return doFlowModDelete(fm, false);
		case DELETE_STRICT:
			return doFlowModDelete(fm,true);
		default:
			return false;
		}
	}
	
	public PhysicalFlowEntry getPhysicalFlowEntry (Long cookie) {
		if(this.hasEntry(cookie)){
			return entryMap.get(cookie);
		}
		return null;
	}
	
	public long addFlowMod(OVXFlowMod flowmod, long cookie) {
		if(flowmod.getFlowMod().getCommand()==OFFlowModCommand.ADD){
			doFlowModAdd(flowmod);
			return cookie;
			
		}else{
			log.error("Invalid command");
			return 0;	
			}
	}
	
	public PhysicalSwitch getPhysicalSwitch(){
		return this.psw;
	}

	public void addPCookiePortMap(long pcookie, OVXPort inport, OVXPort outport){
		if(inport != null && outport != null) {
			this.pcookieInPortMap.put(pcookie, inport);
			this.pcookieOutPortMap.put(pcookie, outport);
			log.info("put test - inport {} outport {} / {} {} \n", this.pcookieInPortMap.get(pcookie).getPortNumber(), this.pcookieOutPortMap.get(pcookie).getPortNumber(), inport.getPortNumber(), outport.getPortNumber());
		}
	}

	public OVXPort getInPortFromPCookie(long pcookie){
		return this.pcookieInPortMap.get(pcookie);
	}

	public OVXPort getOutPortFromPCookie(long pcookie){
		return this.pcookieOutPortMap.get(pcookie);
	}

	private boolean doFlowModAdd(OVXFlowMod fm){
		PhysicalFlowEntry pEntry = new PhysicalFlowEntry(fm, this.psw);
		this.entryMap.put(fm.getFlowMod().getCookie().getValue(), pEntry);
		//log.info("Mapsize: {}, increased no: {}", this.entryMap.size(), this.number);
		this.number++;
		return true;
	}
	
	private boolean doFlowModModify(OVXFlowMod fm){
		if(this.hasEntry(fm.getFlowMod().getCookie().getValue())){
			PhysicalFlowEntry pEntry = new PhysicalFlowEntry(fm, this.psw);
			this.entryMap.replace(fm.getFlowMod().getCookie().getValue(), pEntry);
			return true;
		}
		return false;
	}
	
	private boolean doFlowModDelete(OVXFlowMod fm, boolean strict){
		if(this.hasEntry(fm.getFlowMod().getCookie().getValue())){
			this.entryMap.remove(fm.getFlowMod().getCookie().getValue());
			this.number--;
			return true;
		}
		return false;
		
	}
	
	public boolean hasEntry(Long cookie){
		return this.entryMap.containsKey(cookie);
	}

	public Iterator<Long> getEntryIterator(){
	    return this.entryMap.keySet().iterator();
    }


}
