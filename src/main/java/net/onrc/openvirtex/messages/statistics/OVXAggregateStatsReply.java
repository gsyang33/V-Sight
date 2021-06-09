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

import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import net.onrc.openvirtex.messages.OVXStatisticsReply;
import org.projectfloodlight.openflow.protocol.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;

public class OVXAggregateStatsReply extends OVXStatistics implements VirtualizableStatistic {

    private Logger log = LogManager.getLogger(OVXAggregateStatsReply.class.getName());


    protected OFAggregateStatsReply ofAggregateStatsReply;
    protected long packetCount;
    protected long byteCount;
    protected int flowCount;

    public OVXAggregateStatsReply(OFMessage ofMessage) {
        super(OFStatsType.AGGREGATE);
        this.ofAggregateStatsReply = (OFAggregateStatsReply)ofMessage;

        this.packetCount = this.ofAggregateStatsReply.getPacketCount().getValue();
        this.byteCount = this.ofAggregateStatsReply.getByteCount().getValue();
        this.flowCount = (int)this.ofAggregateStatsReply.getFlowCount();
    }

    @Override
    public void virtualizeStatistic(final PhysicalSwitch sw, final OVXStatisticsReply msg) {

        HashMap<Integer, List<OFFlowStatsEntry>> stats = new HashMap<Integer, List<OFFlowStatsEntry>>();
        OFAggregateStatsReply ofAggregatedStatsReply = (OFAggregateStatsReply)msg.getOFMessage();
        log.info("[Aggregated stats] xid {} byte {} packet {} ", ofAggregatedStatsReply.getXid(), ofAggregatedStatsReply.getByteCount().getValue(), ofAggregatedStatsReply.getPacketCount().getValue());

    }

    @Override
    public int hashCode() {
        return this.ofAggregateStatsReply.hashCode();
    }
}
