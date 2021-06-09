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
package net.onrc.openvirtex.messages.statistics;

import net.onrc.openvirtex.elements.address.IPAddress;
import net.onrc.openvirtex.elements.address.OVXIPAddress;
import net.onrc.openvirtex.elements.address.PhysicalIPAddress;
import net.onrc.openvirtex.elements.datapath.OVXFlowTable;
import net.onrc.openvirtex.elements.datapath.OVXSingleSwitch;
import net.onrc.openvirtex.elements.datapath.OVXSwitch;
import net.onrc.openvirtex.elements.datapath.PhysicalFlowEntry;
import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.elements.network.PhysicalNetwork;
import net.onrc.openvirtex.elements.network.PhysicalNetworkScheduler;
import net.onrc.openvirtex.elements.port.OVXPort;
import net.onrc.openvirtex.elements.port.PhysicalPort;
import net.onrc.openvirtex.exceptions.*;
import net.onrc.openvirtex.messages.OVXFlowMod;
import net.onrc.openvirtex.messages.OVXStatisticsReply;
import net.onrc.openvirtex.messages.OVXStatisticsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.instruction.OFInstruction;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.projectfloodlight.openflow.types.U64;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Currently, we do not support multi-table and group.
public class OVXFlowStatsRequest extends OVXStatistics implements DevirtualizableStatistic {
    Logger log = LogManager.getLogger(OVXFlowStatsRequest.class.getName());

    protected OFFlowStatsRequest ofFlowStatsRequest;
    protected OFPort outPort;
    // variables for group are missed
    protected Match match;
    protected TableId tableId;


    public OVXFlowStatsRequest(OFMessage ofMessage) {
        super(OFStatsType.FLOW);
        this.ofFlowStatsRequest = (OFFlowStatsRequest)ofMessage;
        this.outPort = this.ofFlowStatsRequest.getOutPort();
        this.match = this.ofFlowStatsRequest.getMatch();
        this.tableId = this.ofFlowStatsRequest.getTableId();
    }

    @Override
    public void devirtualizeStatistic(final OVXSwitch  sw, final OVXStatisticsRequest msg) {
    	//int xid = sw.getTenantId() << 16;
    	int rid = sw.getTenantId();	
    	
        List<OFFlowStatsEntry> replies = new LinkedList<OFFlowStatsEntry>();
        OVXFlowTable vft = (OVXFlowTable) sw.getFlowTable();
       
        if (this.outPort.getPortNumber() != OFPort.ANY.getPortNumber()) {
        	// a specific port no.
        	//this.log.info("[Specific Flow] start");
        	
        	Long vcookie = this.ofFlowStatsRequest.getCookie().getValue();
        	Long pcookie = vft.getPhysicalCookie(vcookie);
        	
        	for (PhysicalSwitch psw : getPhysicalSwitches(sw)) {
        		if(psw.getPhysicalFlowTable().hasEntry(pcookie)){
        			PhysicalFlowEntry pEntry = psw.getPhysicalFlowTable().getPhysicalFlowEntry(pcookie);
    				OFFlowStatsEntry pstat = pEntry.getFlowStatistics(rid);
    				
        			//this.log.info("[Specific Flow]] Got the stats!! >> pcookie : {}, vcookie : {}, data : {}", pcookie, vcookie, pstat.getPacketCount().getValue());
            		if(virtualizeFlowStatistic(sw, pcookie, pstat, replies) == true)
            			break;
        		}
        	}
        } else {
        	// for all entries in the vft.
        	//this.log.info("[Entire Flow] start");
        	
        	Iterator<Long> iter = vft.getVPcookiemap().keySet().iterator();
        	List<PhysicalSwitch> psws = getPhysicalSwitches(sw);
        	
        	while(iter.hasNext()){
        		Long vcookie = iter.next();
        		Long pcookie = vft.getPhysicalCookie(vcookie);
        		
        		for (PhysicalSwitch psw : psws) {
            		if(psw.getPhysicalFlowTable().hasEntry(pcookie)){
            			PhysicalFlowEntry pEntry = psw.getPhysicalFlowTable().getPhysicalFlowEntry(pcookie);

						OFFlowStatsEntry pstat = pEntry.getFlowStatistics(rid);
                		if(virtualizeFlowStatistic(sw, pcookie, pstat, replies) == true)
                			break;
            		}
            	}
        	}
        }
                
        OFFlowStatsReply flowStatsReply = OFFactories.getFactory(sw.getOfVersion()).buildFlowStatsReply()
                .setXid(msg.getOFMessage().getXid())
                .setEntries(replies)
                .build();

        OVXStatisticsReply reply = new OVXStatisticsReply(flowStatsReply);

        sw.sendMsg(reply, sw);
        
    }

    private boolean virtualizeFlowStatistic(OVXSwitch sw, Long pcookie, OFFlowStatsEntry pstat, List<OFFlowStatsEntry> replies){
		OVXFlowMod origFM = null;
		OFFactory ofFactory = OFFactories.getFactory(OFVersion.OF_13);
		OFFlowStatsEntry vstat = ofFactory.buildFlowStatsEntry().build();
		
    	try{
			origFM = sw.getFlowMod(pcookie);
		} catch (MappingException e) {
			log.info("[virtualizeFlowStatistic] FlowMod not found in VFlowTable for vcookie = {}, {}", pcookie, this.ofFlowStatsRequest.toString());
			return false;
		}
    	
//    	log.info("Table ID {} Duration {} Cookie {} Match {}", 
//    			  origFM.getFlowMod().getTableId(), pstat.getDurationSec(), origFM.getFlowMod().getCookie().getValue(), origFM.getFlowMod().getMatch().toString());


		// [ksyang] edge processing implementation required.
    	vstat = vstat.createBuilder()
    			.setTableId(origFM.getFlowMod().getTableId())
    			.setDurationSec(pstat.getDurationSec())
    			.setDurationNsec(pstat.getDurationNsec())
    			.setPriority(origFM.getFlowMod().getPriority())
    			.setIdleTimeout(origFM.getFlowMod().getIdleTimeout())
    			.setHardTimeout(origFM.getFlowMod().getHardTimeout())
    			.setCookie(origFM.getFlowMod().getCookie())
    			.setPacketCount(pstat.getPacketCount())
    			.setByteCount(pstat.getByteCount())
    			.setMatch(origFM.getFlowMod().getMatch())
    			.setInstructions(origFM.getFlowMod().getInstructions())
    			.setFlags(origFM.getFlowMod().getFlags())
    			.build();
    	//log.info("[virtualizeFlowStatistic] inserting the stats"); // : {}", vstat.toString());
		replies.add(vstat);
		
		return true;
		
//		if(msg.getOFMessage().getVersion() == OFVersion.OF_10)
//            stat = stat.createBuilder().setActions(origFM.getFlowMod().getActions()).build();
//      else
//            vstat = vstat.createBuilder().setInstructions(origFM.getFlowMod().getInstructions()).build();
            
    }


    private OVXIPAddress getVirtualIP(OVXSwitch vs, PhysicalIPAddress pIP){
    	try {
			return vs.getMap().getVirtualIP(pIP);
		}catch (AddressMappingException e) {
    		return null;
		}
	}

    private List<PhysicalSwitch> getAllPhysicalSwitches(OVXSwitch sw){
		LinkedList<PhysicalSwitch> sws = new LinkedList<PhysicalSwitch>();
		try{
    		for (OVXSwitch ovxs : sw.getMap().getVirtualNetwork(sw.getTenantId()).getSwitches()){
				sws.addAll(this.getPSwitch(ovxs));
			}
		}catch (NetworkMappingException e){
    		return null;
		}
		//log.info("pSWitchlist: {}", sws.toString());
		return sws;
	}

	private List<PhysicalSwitch> getPSwitch(OVXSwitch sw){
		if (sw instanceof OVXSingleSwitch) {
			try {
				return sw.getMap().getPhysicalSwitches(sw);
			} catch (SwitchMappingException e) {
				log.debug("OVXSwitch {} does not map to any physical switches",
						sw.getSwitchName());
				return new LinkedList<>();
			}
		}
		return null;
	}

    private List<PhysicalSwitch> getPhysicalSwitches(OVXSwitch sw) {
        if (sw instanceof OVXSingleSwitch) {
            try {
                return sw.getMap().getPhysicalSwitches(sw);
            } catch (SwitchMappingException e) {
                log.debug("OVXSwitch {} does not map to any physical switches",
                        sw.getSwitchName());
                return new LinkedList<>();
            }
        }
        LinkedList<PhysicalSwitch> sws = new LinkedList<PhysicalSwitch>();
        for (OVXPort p : sw.getPorts().values()) {
            if (!sws.contains(p.getPhysicalPort().getParentSwitch())) {
                sws.add(p.getPhysicalPort().getParentSwitch());
            }
        }
        return sws;
    }

    @Override
    // return hashCode
    public int hashCode() {
        return this.ofFlowStatsRequest.hashCode();
    }
}
