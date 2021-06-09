package net.onrc.openvirtex.elements.datapath.statistics;

import java.util.Date;

import net.onrc.openvirtex.elements.network.PhysicalNetwork;
import net.onrc.openvirtex.elements.network.PhysicalNetworkScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowStatsRequest;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.types.TableId;
import net.onrc.openvirtex.core.io.OVXSendMsg;
import net.onrc.openvirtex.elements.datapath.PhysicalFlowEntry;
import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.messages.OVXFlowMod;
import net.onrc.openvirtex.messages.OVXMessage;
import net.onrc.openvirtex.messages.OVXStatisticsRequest;

public class SingleFlowStatMonitor extends BasicMonitoringEntity implements OVXSendMsg{
    Logger log = LogManager.getLogger(SingleFlowStatMonitor.class.getName());

    private PhysicalSwitch sw;
    private OVXFlowMod fm;
    private PhysicalFlowEntry fe;
    //private Integer refreshInterval = 30
    OFFactory ofFactory;
    private final double agingTh = 0.3;
    protected PhysicalNetworkScheduler scheduler;

    public SingleFlowStatMonitor(PhysicalSwitch sw, OVXFlowMod fm, PhysicalFlowEntry fe) {
        /*
         * Get the timer from the PhysicalNetwork class.
         */
    	super(EntityType.SINGLE_FLOW_STAT);
    	this.fe = fe;
        this.sw = sw;
        this.fm = fm;
        this.jobSize = 1;
        //this.refreshInterval = OpenVirteXController.getInstance().getFlowStatsRefresh();
        //this.ofFactory = OFFactories.getFactory(OFVersion.OF_10);
        this.ofFactory = OFFactories.getFactory(sw.getOfVersion());
        this.scheduler = PhysicalNetwork.getScheduler();
    }
    private synchronized void sendFlowStatistics(int tid, short port) {
        //int xid = (tid << 16) | port;
    	//int xid = tid << 16;

        /*
        if(fe.getHitrate() < agingTh){
            //aging - deschedule
            if(fe.isPremonitored()){
                //deschedule
                this.scheduler.removeTask(fe.getPremonitor());
            }
            return;
        }
        */

    	int xid = 995; // original
    	OFFlowStatsRequest ofFlowStatsRequest = this.ofFactory.buildFlowStatsRequest()
                .setXid(xid)
                .setMatch(this.fm.getFlowMod().getMatch())
                .setOutPort(this.fm.getFlowMod().getOutPort())
                .setTableId(TableId.ALL)
                .build();
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
	public void startPreMonitoring(){
		fe.startCache();
	}
	
	@Override
	public void endPreMonitoring(){
        log.info("set the status of the single flow job into off");
        fe.stopCache();
	}
	
	@Override
	public void run() {
        sendFlowStatistics(0, (short)0);
	}

    @Override
    public int GetJobSize() {
        return 1;
    }

    @Override
    public int UpdateAndGetJobSize(){
        return 1;
    }

    @Override
    public boolean getScheduleStatus(){
        return this.fe.isPremonitored();
    }

    @Override
    public double getHitrate(){
        return this.fe.getHitrate();
    }

    public void setAggregateHandler(){
        this.fe.setAggregateMonitoring();
    }

    @Override
    public boolean getAggregatedScheduleStatus(){
        return this.sw.getAggregatedFlowMonitoringStatus();
    }

    //@Override
    //public Integer getID(){
    //    return this.fe.getFlowMod().getFlowMod().getCookie().hashCode();
    //}

}