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
package net.onrc.openvirtex.elements.datapath;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.onrc.openvirtex.elements.datapath.statistics.VirtualPortStatMonitor;
import net.onrc.openvirtex.elements.port.OVXPort;
import net.onrc.openvirtex.messages.OVXFlowMod;
import net.onrc.openvirtex.messages.OVXMessageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import net.onrc.openvirtex.exceptions.MappingException;
import net.onrc.openvirtex.exceptions.SwitchMappingException;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U32;
import org.projectfloodlight.openflow.types.U64;



/**
 * Virtualized version of the switch flow table.
 */
public class OVXFlowTable implements FlowTable {

    private final Logger log = LogManager.getLogger(OVXFlowTable.class.getName());

    // OVXSwitch tied to this table
    protected OVXSwitch vswitch;
    // Map of FlowMods to physical cookies for vlinks
    protected ConcurrentHashMap<Long, OVXFlowMod> flowmodMap;
    // Reverse map of FlowMod hashcode to cookie - flowModHash, pCookie
    protected ConcurrentHashMap<Integer, Long> cookieMap;
    // Map of OVXFlowEntry
    protected ConcurrentHashMap<Long, OVXFlowEntry> flowEntryMap;
    // Map of v-p cookies (ksyang - 이 파트의 쿠키맵으로 위에 있는 쿠키맵을 완전히 대체하면 되는데, 귀찮아서 그냥 따로 둠) // [dhjeon] key = vcookie
    protected ConcurrentHashMap<Long, Long> vpcookieMap;
    // Map for Tx statistics gathering (ksyang)
    protected ConcurrentHashMap<Short, Map<Long, Long>> outportRuleMap;
    // Map for Rx statistics gathering (ksyang)
    protected ConcurrentHashMap<Short, Map<Long, Long>> inportRuleMap;
    private final double rescheduleTh = 0.7;
    

    /**
     * Temporary solution that should be replaced by something that doesn't
     * fragment.
     */
    private AtomicInteger cookieCounter;

    /**
     * Stores previously used cookies so we only generate one when this list is
     * empty.
     */
    private LinkedList<Long> freeList;
    private static final int FREELIST_SIZE = 1024;

    /* statistics per specs */
    protected int activeEntries;
    protected long lookupCount;
    protected long matchCount;

    /**
     * Instantiates a new flow table associated to the given
     * virtual switch. Initializes flow mod and cookie mappings,
     * and some counters and statistics.
     *
     * @param vsw the virtual switch
     */
    public OVXFlowTable(OVXSwitch vsw) {
        this.flowmodMap = new ConcurrentHashMap<Long, OVXFlowMod>();
        this.cookieMap = new ConcurrentHashMap<Integer, Long>();
        this.flowEntryMap = new ConcurrentHashMap<Long, OVXFlowEntry>();
        this.vpcookieMap = new ConcurrentHashMap<Long, Long>();
        this.outportRuleMap = new ConcurrentHashMap<Short, Map<Long, Long>>();
        this.inportRuleMap = new ConcurrentHashMap<Short, Map<Long, Long>>();
        
        this.cookieCounter = new AtomicInteger(1);
        this.freeList = new LinkedList<Long>();
        this.vswitch = vsw;

         /* initialise stats */
        this.activeEntries = 0;
        this.lookupCount = 0;
        this.matchCount = 0;
    }

    /**
     * Checks if the flow table is empty.
     *
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return this.flowmodMap.isEmpty();
    }

    /**
     * Processes FlowMods according to command field, writing out FlowMods south
     * if needed.
     *
     * @param fm The FlowMod to apply to this table
     * @return if the FlowMod needs to be sent south during devirtualization.
     */
    public boolean handleFlowMods(OVXFlowMod fm) {
        this.log.debug("handleFlowMods");

        switch (fm.getFlowMod().getCommand()) {
            case ADD:
                this.log.debug("ADD");
                return doFlowModAdd(fm);
            case MODIFY:
            case MODIFY_STRICT:
                this.log.debug("MODIFY");
                return doFlowModModify(fm);
            case DELETE:
                this.log.debug("DELETE");
                return doFlowModDelete(fm, false);
            case DELETE_STRICT:
                this.log.debug("DELETE_STRICT");
                return doFlowModDelete(fm, true);
            default:
                return false;
        }
    }

    /**
     * Deletes an existing FlowEntry, expanding out a OFPFW_ALL delete sent
     * initially be a controller. If not, checks for entries, and only allows
     * entries that exist here to be deleted.
     *
     * @param fm the ovxFlowMod
     * @param strict true if a STRICT match
     * @return true if FlowMod should be written south
     */
    private boolean doFlowModDelete(OVXFlowMod fm, boolean strict) {
        /* don't do anything if FlowTable is empty */
        if (this.flowmodMap.isEmpty()) {
            return false;
        }
        try {
            int count = 0;

            for(MatchField field : fm.getFlowMod().getMatch().getMatchFields())
                count++;

            if (count == 0) {
                List<PhysicalSwitch> pList = this.vswitch.getMap()
                        .getPhysicalSwitches(this.vswitch);
                for (PhysicalSwitch psw : pList) {
                    // do FlowMod cleanup like when port dies.
                    psw.cleanUpTenant(this.vswitch.getTenantId(),
                            OFPort.ANY.getShortPortNumber());
                }
                this.flowmodMap.clear();
                this.cookieMap.clear();
                this.vpcookieMap.clear();
                this.outportRuleMap.clear();
                this.inportRuleMap.clear();
                return false;
            } else {
                // remove matching flow entries, and let FlowMod be sent down((OFFlowMod)fm.getOFMessage()).getMatch()
                Iterator<Map.Entry<Long, OVXFlowMod>> itr = this.flowmodMap
                        .entrySet().iterator();
                OVXFlowEntry fe = new OVXFlowEntry();
                while (itr.hasNext()) {
                    Map.Entry<Long, OVXFlowMod> entry = itr.next();
                    fe.setOVXFlowMod(entry.getValue());
                    int overlap = fe.compare(fm.getFlowMod().getMatch(), strict);
                    if (overlap == OVXFlowEntry.EQUAL) {
                    	this.vpcookieMap.remove(entry.getValue().getFlowMod().getCookie().getValue());
                        if(this.outportRuleMap.get(entry.getValue().getFlowMod().getOutPort().getShortPortNumber()) != null
                                && this.inportRuleMap.get(entry.getValue().getFlowMod().getMatch().get(MatchField.IN_PORT).getShortPortNumber()) != null) {
                            this.outportRuleMap.get(entry.getValue().getFlowMod().getOutPort().getShortPortNumber())
                                    .remove(entry.getValue().getFlowMod().getCookie().getValue());
                            this.inportRuleMap.get(entry.getValue().getFlowMod().getMatch().get(MatchField.IN_PORT).getShortPortNumber())
                                    .remove(entry.getValue().getFlowMod().getCookie().getValue());
                        }
                        this.cookieMap.remove(entry.getValue().hashCode());
                        itr.remove();
                    }
                }
                return true;
            }
        } catch (SwitchMappingException e) {
            log.warn("Could not clear PhysicalSwitch tables: {}", e);
        }
        return false;
    }



    /**
     * Adds a flow entry to the FlowTable. The FlowMod is checked for overlap if
     * its flag says so.
     *
     * @param fm the flow mod
     * @return true if FlowMod should be written south
     */
    private boolean doFlowModAdd(OVXFlowMod fm) {
        //this.log.info("doFlowModAdd");
        //this.log.info("doFlowModAdd - {}",fm.getOFMessage().toString());


        if (fm.getFlowMod().getFlags().contains(OFFlowModFlags.CHECK_OVERLAP)){
            //System.out.println(" OFPFF_CHECK_OVERLAP");

            OVXFlowEntry fe = new OVXFlowEntry();
            for (OVXFlowMod fmod : this.flowmodMap.values()) {

                fe.setOVXFlowMod(fmod);
                int res = fe.compare(fm.getFlowMod().getMatch(), false);

                //System.out.println("------------------------");
                //System.out.println(res);

                //System.out.println(fm.getFlowMod().getPriority() + " " + fe.getPriority());

//                this.log.info("res : " + res);
                if ((res != OVXFlowEntry.DISJOINT)
                        & (fm.getFlowMod().getPriority() == fe.getPriority())) {

                    //System.out.println("true");

                    this.vswitch.sendMsg(OVXMessageUtil.makeErrorMsg(
                            OFFlowModFailedCode.OVERLAP, fm),
                            this.vswitch);
                    return false;
                }
            }
        }
        return doFlowModModify(fm);
    }


    /**
     * Adds the FlowMod to the table.
     *
     * @param fm the flow mod
     * @return true if FlowMod should be written South
     */
    private boolean doFlowModModify(OVXFlowMod fm) {
        //this.log.info("doFlowModModify - {}",fm.getOFMessage().toString());

        OVXFlowEntry fe = new OVXFlowEntry();
        int res;
        for (Map.Entry<Long, OVXFlowMod> fmod : this.flowmodMap.entrySet()) {
            fe.setOVXFlowMod(fmod.getValue());

            log.debug(" FlowEntry [" + U32.of(fe.getOVXFlowMod().hashCode()).toString() + "]");

            log.debug("OVXFlowMod [" + U32.of(fm.hashCode()).toString() + "]");

            res = fe.compare(fm.getFlowMod().getMatch(), true);
            //System.out.println("res = " + res);

            //replace table entry that strictly matches with given FlowMod.
            if (res == OVXFlowEntry.EQUAL) {
                //System.out.println("res == OVXFlowEntry2.EQUAL");

                long c = fmod.getKey();
                //System.out.println("replacing equivalent FlowEntry [cookie={}]");
                //log.info("replacing equivalent FlowEntry Cookie={}", U64.of(c).toString());
                OVXFlowMod old = this.flowmodMap.get(c);

                if(old!=null)
                    log.debug("remove old FlowMod [" + U32.of(old.hashCode()).toString() +"]");

                this.vpcookieMap.remove(old.getFlowMod().getCookie().getValue());
                if(this.outportRuleMap.get(old.getFlowMod().getOutPort().getShortPortNumber()) != null
                        && this.inportRuleMap.get(old.getFlowMod().getMatch().get(MatchField.IN_PORT).getShortPortNumber()) != null) {
                    this.outportRuleMap.get(old.getFlowMod().getOutPort().getShortPortNumber())
                            .remove(old.getFlowMod().getCookie().getValue());
                    this.inportRuleMap.get(old.getFlowMod().getMatch().get(MatchField.IN_PORT).getShortPortNumber())
                            .remove(old.getFlowMod().getCookie().getValue());
                }
                this.cookieMap.remove(old.hashCode());
                this.addFlowMod(fm, c);
                // return cookie to pool and use the previous cookie
                return true;
            }else{
                //System.out.println("res != OVXFlowEntry2.EQUAL");
            }
        }
        /* make a new cookie, add FlowMod */
        //System.out.println("make a new cookie, add FlowMod");
        //System.out.println("Cookie = " + this.getCookie());

        long newc = this.getCookie();
        log.debug("make a new [cookie={}]", U64.of(newc).toString());

        this.addFlowMod(fm.clone(), newc);
        return true;
    }


    /**
     * Gets a copy of the FlowMod out of the flow table without removing it.
     *
     * @param cookie the physical cookie
     * @return a clone of the stored FlowMod
     * @throws MappingException if the cookie is not found
     */
    public OVXFlowMod getFlowMod(Long cookie) throws MappingException {
        OVXFlowMod fm = this.flowmodMap.get(cookie);
        if (fm == null) {
            throw new MappingException(cookie, OVXFlowMod.class);
        }
        return fm.clone();
    }

    /**
     * Checks if the cookie is present in the flow table.
     *
     * @param cookie the cookie
     * @return true if cookie is present, false otherwise
     */
    public boolean hasFlowMod(long cookie) {
        return this.flowmodMap.containsKey(cookie);
    }

    /**
     * Gets a new cookie.
     *
     * @return the cookie
     */
    public long getCookie() {
        return this.generateCookie();
    }

    /**
     * Gets a cookie based on the given flow mod.
     *
     * @param flowmod the flow mod
     * @param cflag TODO
     * @return the cookie
     */
    public final long getCookie(OVXFlowMod flowmod, Boolean cflag) {

        if (cflag) {
            long cookie = this.getCookie();
            OVXFlowEntry fe = new OVXFlowEntry();
            int res;
            for (Map.Entry<Long, OVXFlowMod> fmod : this.flowmodMap.entrySet()) {
                fe.setOVXFlowMod(fmod.getValue());
                res = fe.compare(flowmod.getFlowMod().getMatch(), true);
                // replace table entry that strictly matches with given FlowMod.
                if (res == OVXFlowEntry.EQUAL) {
                    long c = fmod.getKey();
                    log.info(
                            "replacing equivalent FlowEntry with new [cookie={}->{}]",
                            U64.of(c).toString(),
                            U64.of(cookie).toString());
                    OVXFlowMod old = this.flowmodMap.get(c);
                    this.cookieMap.remove(old.hashCode());
                    this.flowmodMap.remove(c);
                    this.addFlowMod(flowmod, cookie);
                    // return cookie to pool and use the previous cookie
                    return cookie;
                }
            }
        }
        Long cookie = this.cookieMap.get(flowmod.hashCode());
        if (cookie == null) {
            cookie = this.getCookie();
        }
        log.debug("getCookie Cookie = " + U64.of(cookie).toString());
        log.debug("HashCode = " + U32.of(flowmod.hashCode()).toString());
        log.debug(flowmod.getFlowMod().toString());

        return cookie;
    }

    /**
     * Adds the given flow mod and associate it to the given cookie.
     *
     * @param flowmod the flow mod
     * @param cookie the cookie
     * @return the cookie
     */
    public long addFlowMod(final OVXFlowMod flowmod, long cookie) {
        //this.log.info("addFlowMod - {}", flowmod.toString());
        log.debug("addFlowMod Cookie = " + U64.of(cookie).toString());
        log.debug("HashCode = " + U32.of(flowmod.getFlowMod().hashCode()).toString());
        log.debug(flowmod.getFlowMod().toString());
        
        OVXFlowEntry fe = new OVXFlowEntry();
        fe.setOVXFlowMod(flowmod);
        
        this.flowmodMap.put(cookie, flowmod);
        this.cookieMap.put(flowmod.hashCode(), cookie);
        this.flowEntryMap.put(cookie, fe);
        this.vpcookieMap.put(flowmod.getFlowMod().getCookie().getValue(), cookie);



        /* ksyang - Maps for portstats */
        short outPortNum, inPortNum;
                OVXPort ovxOutPort = null, ovxInPort = null;
//        for (final OFAction act : flowmod.getFlowMod().getActions()){
//            if(act.getType() == OFActionType.OUTPUT){
//                OFActionOutput outputAct = (OFActionOutput) act;
//                outPortNum = outputAct.getPort().getShortPortNumber();
//                ovxOutPort = this.vswitch.getPort(outPortNum);
//                if(!this.outportRuleMap.containsKey(outPortNum)) {
//                    this.outportRuleMap.put(outPortNum, new HashMap<Long, Long>());
//                }
//                this.outportRuleMap.get(outPortNum).put(flowmod.getFlowMod().getCookie().getValue(), cookie);
//                break;
//            }
//        }
//        inPortNum = flowmod.getFlowMod().getMatch().get(MatchField.IN_PORT).getShortPortNumber();
//        //this.log.info("inPortNum: {}", inPortNum);
//        if(!this.inportRuleMap.containsKey(inPortNum)) {
//            this.inportRuleMap.put(inPortNum, new HashMap<Long, Long>());
//        }
//        ovxInPort = this.vswitch.getPort(inPortNum);
//        this.inportRuleMap.get(inPortNum).put(flowmod.getFlowMod().getCookie().getValue(), cookie);

        for (final OFAction act : flowmod.getFlowMod().getActions()){

            if(act.getType() == OFActionType.OUTPUT){
                OFActionOutput outputAct = (OFActionOutput) act;
                outPortNum = outputAct.getPort().getShortPortNumber();
                ovxOutPort = this.vswitch.getPort(outPortNum);

                inPortNum = flowmod.getFlowMod().getMatch().get(MatchField.IN_PORT).getShortPortNumber();
                ovxInPort = this.vswitch.getPort(inPortNum);

                if(ovxOutPort != null && ovxInPort != null) {

                    if (!this.inportRuleMap.containsKey(inPortNum)) {
                        this.inportRuleMap.put(inPortNum, new HashMap<Long, Long>());
                    }

                    this.inportRuleMap.get(inPortNum).put(flowmod.getFlowMod().getCookie().getValue(), cookie);


                    if (!this.outportRuleMap.containsKey(outPortNum)) {
                        this.outportRuleMap.put(outPortNum, new HashMap<Long, Long>());
                    }
                    this.outportRuleMap.get(outPortNum).put(flowmod.getFlowMod().getCookie().getValue(), cookie);
                }
                break;
            }
        }





        // gsyang - rescheduling of vPortPreMonitor, should implement reschedule routine but ignore the single pre-monitoring bypass for port stats.
        calculateJobandDeschedule(ovxOutPort);
        calculateJobandDeschedule(ovxInPort);

        //this.log.info("inport - {}, outport - {}", this.inportRuleMap.get(flowmod.getFlowMod().getMatch().get(MatchField.IN_PORT).getShortPortNumber()).get(flowmod.getFlowMod().getCookie().getValue())
        //, this.outportRuleMap.get(outPortNum).get(flowmod.getFlowMod().getCookie().getValue()));
        return cookie;
    }

    private void calculateJobandDeschedule(OVXPort ovxPort) {
        if(ovxPort != null){
            VirtualPortStatMonitor monitor = (VirtualPortStatMonitor)ovxPort.getPremonitor();
            if(monitor.getScheduled()) {
                this.log.info("Current job size: {}, rate: {}", monitor.GetJobSize(), (double) (monitor.GetJobSize() + 1) / (double) monitor.GetJobSize() > rescheduleTh);
                if (monitor.GetJobSize() == 0 || (double) (monitor.GetJobSize() + 1) / (double) monitor.GetJobSize() > rescheduleTh) {
                    this.log.info("Deschedule {}", monitor.toString());
                    //monitor.descheduleMe();
                }
            }
        }
    }

    /**
     * Deletes the flow mod associated with the given cookie.
     *
     * @param cookie the cookie
     * @return the flow mod
     */
    public OVXFlowMod deleteFlowMod(final Long cookie) {
        synchronized (this.freeList) {
            if (this.freeList.size() <= OVXFlowTable.FREELIST_SIZE) {
                // add/return cookie to freelist IF list is below FREELIST_SIZE
                this.freeList.add(cookie);
            } else {
                // remove head element, then add
                this.freeList.remove();
                this.freeList.add(cookie);
            }
            OVXFlowMod ret = this.flowmodMap.remove(cookie);
            if (ret != null) {
                this.cookieMap.remove(ret.hashCode());
            }
            return ret;
        }
    }

    /**
     * Fetches a usable cookie for FlowMod storage. If no cookies are available,
     * generate a new physical cookie from the OVXSwitch tenant ID and
     * OVXSwitch-unique cookie counter.
     *
     * @return a physical cookie
     */
    private long generateCookie() {
        try {
            return this.freeList.remove();
        } catch (final NoSuchElementException e) {
            // none in queue - generate new cookie
            // TODO double-check that there's no duplicate in ovxFlowMod map.
            final int cookie = this.cookieCounter.getAndIncrement();
            return (long) this.vswitch.getTenantId() << 32 | cookie;
        }
    }

    /**
     * Dumps the contents of the FlowTable.
     */
    public void dump() {
        String ret = "\n";
        for (final Map.Entry<Long, OVXFlowMod> fe : this.flowmodMap.entrySet()) {
            ret += "cookie[" + U64.of(fe.getKey()).toString() + "] HashCode [" + U32.of(fe.getValue().hashCode()).toString() + "] :"
                    + fe.getValue().getFlowMod().toString()
                    + "\n";
        }
        this.log.info("OVXFlowTable \n========================\n" + ret
                + "========================\n");
    }

    /**
     * Gets an unmodifiable view of the flow table.
     *
     * @return the flow table
     */
    public Collection<OVXFlowMod> getFlowTable() {
        return Collections.unmodifiableCollection(this.flowmodMap.values());
    }
    
    public Long getPhysicalCookie(Long vcookie){
    	return this.vpcookieMap.get(vcookie);
    }
    
    public ConcurrentHashMap<Long, Long> getVPcookiemap(){
    	return this.vpcookieMap;
    }
/*
    public ConcurrentHashMap<Short, Map<Long, Long>> getOutportRuleMap(){
        return this.outportRuleMap;

    }

    public ConcurrentHashMap<Short, Map<Long, Long>> getInportRuleMap(){
        return this.inportRuleMap;
    }
*/


    public ConcurrentHashMap<Short, Map<Long, Long>> getOutportRuleMap(){
        ConcurrentHashMap<Short, Map<Long, Long>> newHashMap = new ConcurrentHashMap<>(this.outportRuleMap);
        return newHashMap;
    }

    public ConcurrentHashMap<Short, Map<Long, Long>> getInportRuleMap(){
        ConcurrentHashMap<Short, Map<Long, Long>> newHashMap = new ConcurrentHashMap<>(this.inportRuleMap);
        return newHashMap;
    }

}
