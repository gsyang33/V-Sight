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
 *  Libera HyperVisor development based on OpenVirteX
 *
 *   OpenFlow Version Up with OpenFlowj
 *
 *  This is updated by Libera Project team in Korea University
 *
 * Author: Gyeongsik Yang (ksyang@os.korea.ac.kr), Seong-Mun KIm
 ******************************************************************************/
package net.onrc.openvirtex.messages.statistics;

import net.onrc.openvirtex.elements.datapath.PhysicalFlowEntry;
import net.onrc.openvirtex.elements.datapath.PhysicalFlowTable;
import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.elements.network.PhysicalNetwork;
import net.onrc.openvirtex.elements.network.PhysicalNetworkScheduler;
import net.onrc.openvirtex.elements.port.OVXPort;
import net.onrc.openvirtex.messages.OVXStatisticsReply;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U64;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class OVXFlowStatsReply extends OVXStatistics implements VirtualizableStatistic {

    Logger log = LogManager.getLogger(OVXFlowStatsReply.class.getName());

    protected OFFlowStatsReply ofFlowStatsReply;
    protected OFFlowStatsEntry ofFlowStatsEntry;


    public OVXFlowStatsReply(OFMessage ofMessage) {
        super(OFStatsType.FLOW);
        this.ofFlowStatsReply = (OFFlowStatsReply)ofMessage;
        this.ofFlowStatsEntry = null;
    }

    public OVXFlowStatsReply(OFMessage ofMessage, OFFlowStatsEntry ofFlowStatsEntry) {
        super(OFStatsType.FLOW);
        this.ofFlowStatsReply = (OFFlowStatsReply)ofMessage;
        this.ofFlowStatsEntry = ofFlowStatsEntry;
    }

    public void setOFMessage(OFMessage ofMessage) {
        this.ofFlowStatsReply = (OFFlowStatsReply)ofMessage;
    }

    public OFMessage getOFMessage() {
        return this.ofFlowStatsReply;
    }

    public OFFlowStatsEntry getOFFlowStatsEntry() {
        return this.ofFlowStatsEntry;
    }

    @Override
    public void virtualizeStatistic(final PhysicalSwitch sw, final OVXStatisticsReply msg) {
//        this.log.info("[virtualizeStatistic] {}", msg.getOFMessage().toString());

//        if (msg.getOFMessage().getXid() != 0) {
//            sw.removeFlowMods(msg);
//            return;
//        }

        HashMap<Integer, List<OFFlowStatsEntry>> stats = new HashMap<Integer, List<OFFlowStatsEntry>>();
        OFFlowStatsReply ofFlowStatsReply = (OFFlowStatsReply)msg.getOFMessage();
        PhysicalFlowTable pft = sw.getPhysicalFlowTable();

        //log.info("GetXID: {}", ofFlowStatsReply.getXid());
//        if(ofFlowStatsReply.getXid()>=10 && ofFlowStatsReply.getXid()<=200){
//            for (OFFlowStatsEntry stat : ofFlowStatsReply.getEntries()) {
//                this.log.info("Disaggregated accuracy flow Physical data cookie: {}, xid: {}, byte: {} packet: {}",stat.getCookie(), ofFlowStatsReply.getXid(), stat.getByteCount().getValue(), stat.getPacketCount().getValue());
//            }
//            return;
//        }

        //[KSYANG] - upper - for stats virt for aggreagated flow


        if(ofFlowStatsReply.getXid()!=998 && ofFlowStatsReply.getXid() != 999){
        //        && ofFlowStatsReply.getXid() != 565 ) {
            for (OFFlowStatsEntry stat : ofFlowStatsReply.getEntries()) {
                // FRs that have cookie values "FFFEFFFEFFFEFFFE" are default rules, such as IPv4, IPv6, ARP, BOOTP.
                if (pft.hasEntry(stat.getCookie().getValue()) == true) {
                    PhysicalFlowEntry pEntry = pft.getPhysicalFlowEntry(stat.getCookie().getValue());
                    pEntry.setFlowStatistics(stat);
                    if(ofFlowStatsReply.getXid()!=995) {
                        //this.log.info("Updated flow stats for polling");
                        pft.getPhysicalFlowEntry(stat.getCookie().getValue()).setLock();
                    }
                    //}else{
                    //    this.log.info("Updated flow stats for caching");
                    //}
                }
            }
        }else if(ofFlowStatsReply.getXid() == 999){
            for (OFFlowStatsEntry stat : ofFlowStatsReply.getEntries()){
                OVXPort outPort = pft.getOutPortFromPCookie(stat.getCookie().getValue());
                //this.log.info("Now, Updated port RX - outport {}", outPort.getPortNumber());
                if(outPort != null) {
                    outPort.updateTxPortStat(stat);
                }
                //log.info("port {} current lock {}, ");
                outPort.downLock();

            }
        }else if(ofFlowStatsReply.getXid() == 998){         // Stats for inPort
            for (OFFlowStatsEntry stat : ofFlowStatsReply.getEntries()) {
                OVXPort inPort = pft.getInPortFromPCookie(stat.getCookie().getValue());
                //this.log.info("Now, updated port tx - inport {}", inPort.getPortNumber());
                if(inPort != null) {
                    inPort.updateRxPortStat(stat);
                }
                inPort.downLock();
            }
        }
        /*else if(ofFlowStatsReply.getXid() == 565){         // Stats for fr aggregated sv
            for (OFFlowStatsEntry stat : ofFlowStatsReply.getEntries()) {
                if (pft.hasEntry(stat.getCookie().getValue()) == true) {
                    PhysicalFlowEntry pEntry = pft.getPhysicalFlowEntry(stat.getCookie().getValue());
                    pEntry.setEdgeFlowStatistics(stat);
                    pft.getPhysicalFlowEntry(stat.getCookie().getValue()).setEdgeLock();
                    //}else{
                    //    this.log.info("Updated flow stats for caching");
                    //}
                }
            }
        }*/

        /*else if(ofFlowStatsReply.getXid() == 999){
            for (OFFlowStatsEntry stat : ofFlowStatsReply.getEntries()){
                OVXPort outPort = pft.getOutPortFromPCookie(stat.getCookie().getValue());
                //this.log.info("Updated port RX");
                if(outPort != null) {
                    outPort.updateTxPortStat(stat);
                }

            }
        }else if(ofFlowStatsReply.getXid() == 998){         // Stats for inPort
            for (OFFlowStatsEntry stat : ofFlowStatsReply.getEntries()) {
                OVXPort inPort = pft.getInPortFromPCookie(stat.getCookie().getValue());
                //this.log.info("updated port tx");
                if(inPort != null) {
                    inPort.updateRxPortStat(stat);
                }
            }
        }
        */


    }
    
//    private void addToStats(int tid, OFFlowStatsEntry reply, HashMap<Integer, List<OFFlowStatsEntry>> stats) {
//        List<OFFlowStatsEntry> statsList = stats.get(tid);
//        if (statsList == null) {
//            statsList = new LinkedList<OFFlowStatsEntry>();
//        }
//        statsList.add(reply);
//        stats.put(tid, statsList);
//    }
//
//    private int getTidFromCookie(long cookie) {
//        return (int) (cookie >> 32);
//    }


    @Override
    public int hashCode() {
        return this.ofFlowStatsReply.hashCode();
    }
}
