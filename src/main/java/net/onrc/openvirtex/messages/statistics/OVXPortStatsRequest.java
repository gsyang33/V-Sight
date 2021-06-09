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

import net.onrc.openvirtex.elements.datapath.OVXSwitch;
import net.onrc.openvirtex.elements.datapath.PhysicalFlowEntry;
import net.onrc.openvirtex.elements.port.PhysicalPort;
import net.onrc.openvirtex.elements.port.PhysicalPortEntry;
import net.onrc.openvirtex.elements.port.OVXPort;
import net.onrc.openvirtex.messages.OVXStatisticsReply;
import net.onrc.openvirtex.messages.OVXStatisticsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.types.OFPort;

import java.util.LinkedList;
import java.util.List;

public class OVXPortStatsRequest extends OVXStatistics implements DevirtualizableStatistic {
    Logger log = LogManager.getLogger(OVXPortStatsRequest.class.getName());

    protected OFPortStatsRequest ofPortStatsRequest;
    protected OFPort portNo;

    public OVXPortStatsRequest(OFMessage ofMessage) {
        super(OFStatsType.PORT);
        this.ofPortStatsRequest = (OFPortStatsRequest)ofMessage;

        this.portNo = this.ofPortStatsRequest.getPortNo();
    }

    @Override
    public void devirtualizeStatistic(final OVXSwitch sw, final OVXStatisticsRequest msg) {
    	int xid = sw.getTenantId();	

        List<OFPortStatsEntry> replies = new LinkedList<OFPortStatsEntry>();

        // case for a specific port
        if (this.portNo.getPortNumber() != OFPort.ANY.getPortNumber()) {
        	short vPort = this.portNo.getShortPortNumber();
        	OVXPort p = sw.getPort(vPort);
        	PhysicalPort pP = p.getPhysicalPort();

            OFPortStatsEntry reply = p.getPortStat(xid);
            if(reply != null) {
                replies.add(reply);
            }
            OFPortStatsReply portStatsReply = OFFactories.getFactory(msg.getOFMessage().getVersion()).buildPortStatsReply()
                    .setEntries(replies)
                    .setXid(msg.getOFMessage().getXid())
                    .build();

            OVXStatisticsReply rep = new OVXStatisticsReply(portStatsReply);
            sw.sendMsg(rep, sw);
        }else{
        	// case for any ports
        	//this.log.info("[Entire port] start");
            for (OVXPort p : sw.getPorts().values()) {
            	OFPortStatsEntry reply = p.getPortStat(xid);

                if (reply != null) {
                    /*
                     * Setting it here will also update the reference but this
                     * should not matter since we index our port stats struct by
                     * physical port number (so this info is not lost) and we
                     * always rewrite the port num to the virtual port number.
                     */
                replies.add(reply);
            }
            }

            OFPortStatsReply portStatsReply = OFFactories.getFactory(msg.getOFMessage().getVersion()).buildPortStatsReply()
                    .setEntries(replies)
                    .setXid(msg.getOFMessage().getXid())
                    .build();

            OVXStatisticsReply rep = new OVXStatisticsReply(portStatsReply);

            sw.sendMsg(rep, sw);
        }
    }

    @Override
    public int hashCode() {
        return this.ofPortStatsRequest.hashCode();
    }
}
