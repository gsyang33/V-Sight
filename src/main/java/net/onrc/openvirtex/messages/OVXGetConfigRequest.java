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
package net.onrc.openvirtex.messages;

import net.onrc.openvirtex.elements.datapath.OVXSwitch;
import org.projectfloodlight.openflow.exceptions.OFParseError;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFGetConfigReply;
import org.projectfloodlight.openflow.protocol.OFMessage;

public class OVXGetConfigRequest extends OVXMessage implements Devirtualizable {
    public OVXGetConfigRequest(OFMessage msg) {
        super(msg);
    }

    @Override
    public void devirtualize(final OVXSwitch sw) throws OFParseError {

        OFGetConfigReply reply = this.factory.buildGetConfigReply()
                .setMissSendLen(sw.getMissSendLen())
                .setXid(this.getOFMessage().getXid())
                .build();

        final OVXGetConfigReply ovxGetConfigReply2 = new OVXGetConfigReply(reply);

        sw.sendMsg(ovxGetConfigReply2, sw);
    }
}
