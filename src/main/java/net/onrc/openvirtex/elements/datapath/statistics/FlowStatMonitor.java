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
import org.projectfloodlight.openflow.types.TableId;


public class FlowStatMonitor extends BasicMonitoringEntity implements OVXSendMsg {

    private PhysicalSwitch sw;
    Logger log = LogManager.getLogger(FlowStatMonitor.class.getName());
    OFFactory ofFactory;

    public FlowStatMonitor(PhysicalSwitch sw) {
        /*
         * Get the timer from the PhysicalNetwork class.
         */
    	super(EntityType.AGG_FLOW_STAT);
        this.sw = sw;
        this.ofFactory = OFFactories.getFactory(OFVersion.OF_13);

        //log.info("Now creating aggregated flow statistics retrieving task - for switch {}", this.sw.toString());
    }
    private void sendFlowStatistics(int tid, short port) {
        //int xid = (tid << 16) | port;
    	//int xid = tid << 16;
    	int xid = 995;
    	
        OFFlowStatsRequest ofFlowStatsRequest = this.ofFactory.buildFlowStatsRequest()
                .setXid(xid)
                .setMatch(this.ofFactory.matchWildcardAll())
                .setOutPort(OFPort.ANY)
                .setTableId(TableId.ALL)
                .build();

        //this.log.info("Raw message: {}, xid: {}", ofFlowStatsRequest.toString(), ofFlowStatsRequest.getXid());

        OVXStatisticsRequest req = new OVXStatisticsRequest(ofFlowStatsRequest);

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
	public PhysicalSwitch getPhysicalSwtich() {
		return this.sw;
	}
	@Override
	public void run() {
		//log.info("WILDCARDED STATISTICS! {}", this.sw.getSwitchName());
        sendFlowStatistics(0, (short) 0);
	}

	@Override
    public void startPreMonitoring(){

        this.sw.onAggregatedFlowMonitored();
    }

    @Override
    public void endPreMonitoring(){
        log.info("set the status of the aggregated flowjob into off");
        this.sw.offAggregatedFlowMonitored();
    }

    @Override
    public boolean getScheduleStatus(){

        return this.sw.getAggregatedFlowMonitoringStatus();
    }

    @Override
    public double getHitrate(){
        return 0.9;
    }

    @Override
    public int GetJobSize(){
        //this.log.info("Retrived the Aggregated job task size: {}", this.sw.getPhysicalFlowTable().getEntryNumber());
        return this.sw.getPhysicalFlowTable().getEntryNumber();
    }

    @Override
    public int UpdateAndGetJobSize(){
        return this.sw.getPhysicalFlowTable().getEntryNumber();
    }
}
