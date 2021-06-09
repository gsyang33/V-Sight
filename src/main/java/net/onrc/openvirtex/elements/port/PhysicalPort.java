/*******************************************************************************
 * Copyright 2014 Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ****************************************************************************
 * Libera HyperVisor development based OpenVirteX for SDN 2.0
 *
 *   OpenFlow Version Up with OpenFlowj
 *
 * This is updated by Libera Project team in Korea University
 *
 * Author: Seong-Mun Kim (bebecry@gmail.com)
 ******************************************************************************/
package net.onrc.openvirtex.elements.port;

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

/**
 * A physical port maintains the mapping of all virtual ports that are mapped to
 * it.
 */
public class PhysicalPort extends Port<PhysicalSwitch, PhysicalLink> {
    Logger log = LogManager.getLogger(PhysicalPort.class.getName());


    private Map<Integer, HashMap<Integer, OVXPort>> ovxPortMap;
    private ConcurrentHashMap<Long, PhysicalPortEntry> entryMap;
    private ConcurrentHashMap<Long, OFPortStatsEntry> portStats;
	private StatInterval statInterval;
	private BasicMonitoringEntity portMonitor;
		
    /**
     * Instantiates a physical port based on an OpenFlow physical port.
     *
     * @param port
     *            the OpenFlow physical port
     */
    private PhysicalPort(final OFPortDesc port) {
        super(port);
        this.ovxPortMap = new HashMap<Integer, HashMap<Integer, OVXPort>>();
        this.portStats = new ConcurrentHashMap<Long, OFPortStatsEntry>();

//		
//		try {
//			MeanDev md = getStatInterval().getMeanDev(); 
//			double defaultInterval = (double)OpenVirteXController.getInstance().getFlowStatsRefresh() * 1000;
//			PhysicalNetwork.getScheduler().addTask(portMonitor, defaultInterval, 0.0);
//		} catch (IndexOutOfBoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    }

    /**
     * Instantiates a physical port based on an OpenFlow physical port, the
     * physical switch it belongs to, and set whether the port is an edge
     * port or not.
     *
     * @param port
     *            the OpenFlow physical port
     * @param sw
     *            the physical switch
     * @param isEdge
     *            edge attribute
     */
    public PhysicalPort(final OFPortDesc port, final PhysicalSwitch sw,
                        final boolean isEdge) {
    	this(port);
    	this.parentSwitch = sw;
    	this.entryMap = new ConcurrentHashMap<Long, PhysicalPortEntry>();
//    	this.ofFactory = OFFactories.getFactory(port.getVersion()); // [dhjeon]
//		this.statInterval = new StatInterval();
//		this.portMonitor = new SinglePortStatMonitor(this.parentSwitch, this.getPortNumber());

    	this.isEdge = isEdge;
    }

    /**
     * Gets the virtual port that maps this physical port for the given tenant
     * ID and virtual link ID.
     *
     * @param tenantId
     *            the virtual network ID
     * @param vLinkId
     *            the virtual link ID
     *
     * @return the virtual port instance, null if the tenant ID or the virtual
     *         link ID are invalid
     */
    public OVXPort getOVXPort(final Integer tenantId, final Integer vLinkId) {
        if (this.ovxPortMap.get(tenantId) == null) {
            return null;
        }
        OVXPort p = this.ovxPortMap.get(tenantId).get(vLinkId);
        if (p != null && !p.isActive()) {
            return null;
        }
        return p;
    }

    /**
     * Maps the given virtual port to this physical port.
     *
     * @param ovxPort
     *            the virtual port
     */
    public void setOVXPort(final OVXPort ovxPort) {
        if (this.ovxPortMap.get(ovxPort.getTenantId()) != null) {
            if (ovxPort.getLink() != null) {
                this.ovxPortMap.get(ovxPort.getTenantId()).put(
                        ovxPort.getLink().getInLink().getLinkId(), ovxPort);
            } else {
                this.ovxPortMap.get(ovxPort.getTenantId()).put(0, ovxPort);
            }
        } else {
            final HashMap<Integer, OVXPort> portMap = new HashMap<Integer, OVXPort>();
            if (ovxPort.getLink() != null) {
                portMap.put(ovxPort.getLink().getOutLink().getLinkId(), ovxPort);
            } else {
                portMap.put(0, ovxPort);
            }
            this.ovxPortMap.put(ovxPort.getTenantId(), portMap);
        }
    }

    @Override
    public Map<String, Object> getDBIndex() {
        return null;
    }

    @Override
    public String getDBKey() {
        return null;
    }

    @Override
    public String getDBName() {
        return DBManager.DB_VNET;
    }

    @Override
    public Map<String, Object> getDBObject() {
        Map<String, Object> dbObject = new HashMap<String, Object>();
        dbObject.put(TenantHandler.DPID, this.getParentSwitch().getSwitchId());
        dbObject.put(TenantHandler.PORT, this.portNumber);
        return dbObject;
    }

    /**
     * Removes mapping between given virtual port and this physical port.
     *
     * @param ovxPort
     *            the virtual port
     */
    public void removeOVXPort(OVXPort ovxPort) {
        if (this.ovxPortMap.containsKey(ovxPort.getTenantId())) {
            this.ovxPortMap.remove(ovxPort.getTenantId());
        }
    }

    @Override
    public boolean equals(Object that) {
        if (that == null) {
            return false;
        }
        if (this == that) {
            return true;
        }
        if (!(that instanceof PhysicalPort)) {
            return false;
        }

        PhysicalPort port = (PhysicalPort) that;
        return this.portNumber == port.portNumber
                && this.parentSwitch.getSwitchId() == port.getParentSwitch()
                .getSwitchId();
    }

    /**
     * Gets list of virtual ports that map to this physical port for a given
     * tenant ID. If tenant ID is null then returns all virtual ports that map
     * to this port.
     *
     * @param tenant
     *            the tenant ID
     * @return list of virtual ports
     */
    public List<Map<Integer, OVXPort>> getOVXPorts(Integer tenant) {
        List<Map<Integer, OVXPort>> ports = new ArrayList<Map<Integer, OVXPort>>();
        if (tenant == null) {
            ports.addAll(this.ovxPortMap.values());
        } else {
            ports.add(this.ovxPortMap.get(tenant));
        }
        return Collections.unmodifiableList(ports);
    }

    /**
     * Changes the attribute of this port according to a MODIFY PortStatus.
     *
     * @param portstat
     *            the port status
     */
    public void applyPortStatus(OVXPortStatus portstat) {
        /*if (!portstat.isReason(OFPortReason.OFPPR_MODIFY)) {
            return;
        }
        OFPhysicalPort psport = portstat.getDesc();
        this.portNumber = psport.getPortNumber();
        this.hardwareAddress = psport.getHardwareAddress();
        this.name = psport.getName();
        this.config = psport.getConfig();
        this.state = psport.getState();
        this.currentFeatures = psport.getCurrentFeatures();
        this.advertisedFeatures = psport.getAdvertisedFeatures();
        this.supportedFeatures = psport.getSupportedFeatures();
        this.peerFeatures = psport.getPeerFeatures();*/
    }

    /**
     * Unmaps this port from the global mapping and its parent switch.
     */
    public void unregister() {
        // Remove links, if any
        if ((this.portLink != null) && (this.portLink.exists())) {
            this.portLink.egressLink.unregister();
            this.portLink.ingressLink.unregister();
        }
    }

    
/* [dhjeon]
   : moved these functions into the class PhysicalPortEntry
 
 
	public void setPortStatistics(long xid, OFPortStatsEntry stats){
		//this.log.info("entry {} data : {}", xid, stats.toString());
		this.portStats.put(xid, stats);
	}
	
	public OFPortStatsEntry getPortStatistics(long xid){
		long now = new Date().getTime();
		
		this.log.info("[getPortStatistics] ---");
		
		/*
		OFPortStatsRequest ofPortStatsRequest = this.ofFactory.buildPortStatsRequest()
        		//.setXid(xid)
                .setPortNo(OFPort.ANY) //OFPort.ofShort(portNumber)) //OFPort.ANY)
                .build();
        OVXStatisticsRequest req = new OVXStatisticsRequest(ofPortStatsRequest);
        parentSwitch.sendMsg(req, (OVXSendMsg) this);
                
		
		this.log.info("PORT [getPortStaticstics] lastReqTime: {}", lastReqTime);
		// update the interval
		if(this.lastReqTime != 0){
			this.log.info("PORT [getPortStaticstics] interval: {}", now-lastReqTime);
			this.statInterval.addInterval(this.portMonitor, xid, (now - lastReqTime)); // [dhjeon] added an argument "portMonitor"
		}
		
		//this.statInterval.setPrevTime(xid, now);
		this.lastReqTime = now;
		
		this.log.info("PORT stats : {}", this.portStats.get(xid));
		return this.portStats.get(xid);		

	}
	
	public boolean hasStatistics(long xid){
		if(portStats.containsKey(xid)){
			return true;
		}
		return false;
	}
	
	public StatInterval getStatInterval(){
		return this.statInterval;
	}
	*/
}
