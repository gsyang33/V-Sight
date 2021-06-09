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
import net.onrc.openvirtex.elements.datapath.OVXFlowEntry;
import net.onrc.openvirtex.elements.datapath.OVXFlowTable;
import net.onrc.openvirtex.elements.datapath.OVXSwitch;
import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.elements.network.PhysicalNetwork;
import net.onrc.openvirtex.elements.network.PhysicalNetworkScheduler;
import net.onrc.openvirtex.elements.port.OVXPort;
import net.onrc.openvirtex.exceptions.SwitchMappingException;
import net.onrc.openvirtex.messages.OVXFlowMod;
import net.onrc.openvirtex.messages.OVXMessage;
import net.onrc.openvirtex.messages.OVXStatisticsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;


public class VirtualPortStatMonitor extends BasicMonitoringEntity implements OVXSendMsg {
    Logger log = LogManager.getLogger(VirtualPortStatMonitor.class.getName());

    private int trial = 0;
    private OVXSwitch sw;
    private PhysicalSwitch pSw;
    private short port;
    OVXPort vport;
    private Integer refreshInterval = 30;
    private OVXFlowTable ft;
    OFFactory ofFactory;
    private final double agingTh = 0.3;
    protected PhysicalNetworkScheduler scheduler;
    private boolean scheduled = false;

    public VirtualPortStatMonitor(OVXSwitch sw, short port, OVXPort ovxPort) throws SwitchMappingException {
        super(EntityType.SINGLE_PORT_STAT);
        this.sw = sw;
        this.port = port;
        this.vport = ovxPort;
        if(!sw.getMap().getPhysicalSwitches(sw).isEmpty()){
            this.pSw = sw.getMap().getPhysicalSwitches(sw).get(0);
        }

        this.refreshInterval = OpenVirteXController.getInstance().getPortStatsRefresh();
        this.ft = (OVXFlowTable)this.sw.getFlowTable();

        this.ofFactory = OFFactories.getFactory(sw.getOfVersion());
        this.scheduler = PhysicalNetwork.getScheduler();
    }

    public void sendVirtualPortStatistics() {
       // this.log.info("logging from aging routine trial: {}, hitrate: {}", trial, vport.getHitrate());
       // if(trial > 20 && vport.getHitrate() < agingTh){
       //     //aging - deschedule
       //     this.descheduleMe();
       //     return;
       // }

        int inPortXid = 998;
        int outPortXid = 999;

        //log.info("INPORT MAP: {}, OUTPORT MAP: {}", this.ft.getInportRuleMap().get(this.port), this.ft.getOutportRuleMap().get(this.port));

        this.vport.setportStatsTime(new Date().getTime());

        /*log.info("port {} getInportRuleMap null? {}", this.port, this.ft.getInportRuleMap() == null);
        log.info("port {} getOutportRuleMap null? {}", this.port, this.ft.getOutportRuleMap() == null);

        if(this.ft.getInportRuleMap() != null){
            log.info("port {} inport null? {}\n", this.port, this.ft.getInportRuleMap().get(this.port) == null);}

        if(this.ft.getOutportRuleMap() != null){
            log.info("port {} outport null? {}\n", this.port, this.ft.getOutportRuleMap().get(this.port) == null);}
*/
        if(this.ft.getInportRuleMap() != null && this.ft.getInportRuleMap().get(this.port) != null){
            Iterator<Map.Entry<Long, Long>> inPortItr = this.ft.getInportRuleMap().get(this.port).entrySet().iterator();
            portToFlowIterator(inPortXid, inPortItr);
        }

        if(this.ft.getOutportRuleMap() != null && this.ft.getOutportRuleMap().get(this.port) != null){
            Iterator<Map.Entry<Long, Long>> outPortItr = this.ft.getOutportRuleMap().get(this.port).entrySet().iterator();
            portToFlowIterator(outPortXid, outPortItr);
        }



        /* [ ksyang ] modified

        if(this.ft.getInportRuleMap().get(this.port) == null || this.ft.getOutportRuleMap().get(this.port) == null){
            return;
        }
        Iterator<Map.Entry<Long, Long>> inPortItr = this.ft.getInportRuleMap().get(this.port).entrySet().iterator();
        Iterator<Map.Entry<Long, Long>> outPortItr = this.ft.getOutportRuleMap().get(this.port).entrySet().iterator();

        portToFlowIterator(inPortXid, inPortItr);
        portToFlowIterator(outPortXid, outPortItr);*/

    }

    public void checkStatsAccuracy(int xid, OFPortStatsEntry portStatEntry){
        //this.log.info("Disaggregated accuracy port V-Sight data dpid {} port {} xid: {}, RX byte: {} RX packet: {} TX byte: {} TX packet: {}", pSw.getSwitchId(), this.port, xid, portStatEntry.getRxBytes().getValue(), portStatEntry.getRxPackets().getValue(), portStatEntry.getTxBytes().getValue(), portStatEntry.getTxPackets().getValue());
        OFPortStatsRequest ofPortStatsRequest = this.ofFactory.buildPortStatsRequest()
                .setXid(xid)
                .setPortNo(OFPort.ofShort(this.port))
                .build();
        OVXStatisticsRequest req = new OVXStatisticsRequest(ofPortStatsRequest);

        sendMsg(req, this);
    }

    public void sendVirtualPortStatisticsforPolling() {
        // this.log.info("logging from aging routine trial: {}, hitrate: {}", trial, vport.getHitrate());
        // if(trial > 20 && vport.getHitrate() < agingTh){
        //     //aging - deschedule
        //     this.descheduleMe();
        //     return;
        // }

        int inPortXid = 1000;
        int outPortXid = 1001;

        //log.info("INPORT MAP: {}, OUTPORT MAP: {}", this.ft.getInportRuleMap().get(this.port), this.ft.getOutportRuleMap().get(this.port));


        if(this.ft.getInportRuleMap().get(this.port) == null || this.ft.getOutportRuleMap().get(this.port) == null){
            return;
        }
        Iterator<Map.Entry<Long, Long>> inPortItr = this.ft.getInportRuleMap().get(this.port).entrySet().iterator();
        Iterator<Map.Entry<Long, Long>> outPortItr = this.ft.getOutportRuleMap().get(this.port).entrySet().iterator();

        portToFlowIterator(inPortXid, inPortItr);
        portToFlowIterator(outPortXid, outPortItr);

    }

    private void portToFlowIterator(int PortXid, Iterator<Map.Entry<Long, Long>> PortItr) {

        while (PortItr.hasNext()) {
            OFFlowMod fm = pSw.getPhysicalFlowTable().getPhysicalFlowEntry(PortItr.next().getValue()).getFlowMod().getFlowMod();

            OFFlowStatsRequest ofFlowStatsRequest = this.ofFactory.buildFlowStatsRequest()
                    .setXid(PortXid)
                    .setMatch(fm.getMatch())
                    .setOutPort(fm.getOutPort())
                    .setTableId(TableId.ALL)
                    .build();

            OVXStatisticsRequest req = new OVXStatisticsRequest(ofFlowStatsRequest);
            //log.info("Iter req- xid: {}, string: {}", PortXid, req.getOFMessage().toString());
            sendMsg(req, this);
            if(!vport.isPremonitored()){
                vport.upLock();
            }
        }

    }

    @Override
    public void sendMsg(OVXMessage msg, OVXSendMsg from) {
        pSw.sendMsg(msg, from);
    }

    @Override
    public String getName() {
        return "Statistics Manager (" + sw.getName() + ")";
    }

	@Override
	public PhysicalSwitch getPhysicalSwtich() {
		return this.pSw;
	}

    @Override
	public int UpdateAndGetJobSize(){
        int inport = 0, outport = 0;
        if(this.ft.getInportRuleMap().get(this.port) != null){
            inport = this.ft.getInportRuleMap().get(this.port).size();
        }

        if(this.ft.getOutportRuleMap().get(this.port) != null){
            outport = this.ft.getOutportRuleMap().get(this.port).size();
        }
        this.jobSize = inport + outport;
        return jobSize;
    }

    @Override
    public int GetJobSize(){
        return jobSize;
    }

    public synchronized void descheduleMe(){
        if(vport.isPremonitored()){
            // deschedule
            this.scheduler.removeTask(vport.getPremonitor());
            //this.reschedule = true;
            //this.scheduler.addTask(vport.getPremonitor(), this.vport.getStatIntervalMean(), this.vport.getStatIntervalDev());
        }
        return;
    }

    @Override
    public void startPreMonitoring(){
        this.vport.startCache();
        this.scheduled = true;
        this.log.info("vport Monitor.stat: {}", this.scheduled);
    }

    @Override
    public void endPreMonitoring(){
        this.vport.stopCache();
        this.scheduled = false;
    }

    public boolean getScheduled(){
        return this.scheduled;
    }
	@Override
	public void run() {
		//log.info("{{PORT}} =========<{}> Collecting stats for {} - {}", new Date().getTime()%1000000, this.sw.getSwitchName(), this.port); // [dhjeon]
        this.vport.setportStatsTime(new Date().getTime());
        sendVirtualPortStatistics();
        trial ++;
	}

	public OVXSwitch getVirtualSwitch(){
        return this.sw;
    }


}
