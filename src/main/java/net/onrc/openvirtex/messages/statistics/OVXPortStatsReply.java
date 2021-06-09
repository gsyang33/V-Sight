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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.messages.OVXStatisticsReply;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPortStatsEntry;
import org.projectfloodlight.openflow.protocol.OFPortStatsReply;
import org.projectfloodlight.openflow.protocol.OFStatsType;

public class OVXPortStatsReply extends OVXStatistics implements VirtualizableStatistic {
    Logger log = LogManager.getLogger(OVXPortStatsReply.class.getName());

//    private Map<Short, OFPortStatsEntry> stats = null;
    protected OFPortStatsReply ofPortStatsReply;

    public OVXPortStatsReply(OFMessage ofMessage) {
        super(OFStatsType.PORT);
        this.ofPortStatsReply = (OFPortStatsReply)ofMessage;
    }

    @Override
    public void virtualizeStatistic(final PhysicalSwitch sw, final OVXStatisticsReply msg) {
        //this.log.info("[virtualizeStatistic] entered");


//    	stats = new HashMap<Short, OFPortStatsEntry>();
        List<OFPortStatsEntry> statList = ((OFPortStatsReply)msg.getOFMessage()).getEntries();
        for(OFPortStatsEntry stat : statList) {
//            this.log.info("Physical xid {} portstat tenant xid sw {} port {} " +
//                    "RXbyte {} RXpacket {} TXbyte {} TXpacket {}",
//
//                    msg.getOFMessage().getXid(),
//                    sw.getSwitchId(), stat.getPortNo().getPortNumber(),
//                    stat.getRxBytes().getValue(), stat.getRxPackets().getValue(),
//                    stat.getTxBytes().getValue(), stat.getTxPackets().getValue());
        }
        /*
        for(OFPortStatsEntry stat : statList) {
        	if(sw.hasPortEntry(stat.getPortNo().getShortPortNumber())==true){ //
	        	//this.log.info("[REPLY] xid : {}, stat : {}", msg.getOFMessage().getXid(), stat);
	        	sw.getPhysicalPortEntry(stat.getPortNo().getShortPortNumber()).setPortStatistics(stat.getPortNo().getShortPortNumber(), stat); //msg.getOFMessage().getXid(), stat); //[dhjeon]
	        	sw.getPhysicalPortEntry(stat.getPortNo().getShortPortNumber()).setLock(); //[dhjeon]
        	}
        	//sw.getPort(stat.getPortNo().getShortPortNumber()).setPortStatistics(msg.getOFMessage().getXid(), stat); // original
//          stats.put(stat.getPortNo().getShortPortNumber(), stat);
        }
*/

//        sw.setPortStatistics(stats);
    }

    @Override
    public int hashCode() {
        return this.ofPortStatsReply.hashCode();
    }
}
