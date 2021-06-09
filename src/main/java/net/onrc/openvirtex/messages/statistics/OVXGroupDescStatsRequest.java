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
import net.onrc.openvirtex.messages.OVXMessageUtil;
import net.onrc.openvirtex.messages.OVXStatisticsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.OFBadRequestCode;
import org.projectfloodlight.openflow.protocol.OFGroupDescStatsRequest;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFStatsType;

public class OVXGroupDescStatsRequest extends OVXStatistics implements DevirtualizableStatistic {
    private Logger log = LogManager.getLogger(OVXGroupDescStatsRequest.class.getName());

    protected OFGroupDescStatsRequest ofGroupDescStatsRequest;
    public OVXGroupDescStatsRequest(OFMessage ofMessage) {
        super(OFStatsType.GROUP_DESC);

        this.ofGroupDescStatsRequest = (OFGroupDescStatsRequest)ofMessage;
    }

    @Override
    public void devirtualizeStatistic(OVXSwitch sw, OVXStatisticsRequest msg) {
        //This Request message is not supported. (FeaturesReply (Group is unflagged)
        //Send BadRequest Message

        sw.sendMsg(OVXMessageUtil.makeErrorMsg(OFBadRequestCode.BAD_TYPE, msg), sw);
    }
}
