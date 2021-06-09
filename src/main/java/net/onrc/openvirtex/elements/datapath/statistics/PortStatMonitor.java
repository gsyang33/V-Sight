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
package net.onrc.openvirtex.elements.datapath.statistics;

import net.onrc.openvirtex.core.OpenVirteXController;
import net.onrc.openvirtex.core.io.OVXSendMsg;
import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.messages.OVXMessage;
import net.onrc.openvirtex.messages.OVXStatisticsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.types.OFPort;


public class PortStatMonitor extends BasicMonitoringEntity implements OVXSendMsg {

    private PhysicalSwitch sw;
    Logger log = LogManager.getLogger(PortStatMonitor.class.getName());
    private Integer refreshInterval = 30;
    OFFactory ofFactory;

    public PortStatMonitor(PhysicalSwitch sw) {
        /*
         * Get the timer from the PhysicalNetwork class.
         */
    	super(EntityType.AGG_PORT_STAT);
        this.sw = sw;
        this.refreshInterval = OpenVirteXController.getInstance()
                .getPortStatsRefresh();

        this.ofFactory = OFFactories.getFactory(sw.getOfVersion());
    }

    private void sendPortStatistics() {
        // xid 설정 안하나?
    	int xid = 0;
        OFPortStatsRequest ofPortStatsRequest = this.ofFactory.buildPortStatsRequest()
        		.setXid(xid)
                .setPortNo(OFPort.ANY)
                .build();

        OVXStatisticsRequest req = new OVXStatisticsRequest(ofPortStatsRequest);

        sendMsg(req, this);
    }

    @Override
    public void sendMsg(OVXMessage msg, OVXSendMsg from) {
        sw.sendMsg(msg, from);
    }

    @Override
    public String getName() {
        return "Statistics Manager (" + sw.getName() + ")";
    }

	@Override
	public PhysicalSwitch getPhysicalSwtich() {
		return this.sw;
	}

	@Override
	public void run() {
        log.info("Collecting stats for {}", this.sw.getSwitchName());
        sendPortStatistics();		
	}
}
