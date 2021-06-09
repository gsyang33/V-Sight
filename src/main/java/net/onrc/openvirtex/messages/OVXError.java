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
import net.onrc.openvirtex.elements.datapath.PhysicalSwitch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.protocol.errormsg.*;

public class OVXError extends OVXMessage implements Virtualizable, Devirtualizable {
    private final Logger log = LogManager.getLogger(OVXError.class.getName());

    public OVXError(OFMessage msg) {
        super(msg);
    }

    @Override
    public void devirtualize(final OVXSwitch sw) {
        // TODO Auto-generated method stub
    }

    @Override
    public void virtualize(final PhysicalSwitch sw) {
        /*
         * TODO: For now, just report the error. In the future parse them and forward to controller if need be.
         */
        log.error(getErrorString(this));
    }

    private static String getErrorString(OVXError error) {
        // TODO: this really should be OFError.toString. Sigh.
        OFErrorMsg errorMsg = (OFErrorMsg)error.getOFMessage();

        switch (errorMsg.getErrType()) {
            case HELLO_FAILED:
                OFHelloFailedErrorMsg ofHelloFailedErrorMsg = (OFHelloFailedErrorMsg)errorMsg;
                return String.format("Error %s %s",
                        errorMsg.getErrType().toString(),
                        ofHelloFailedErrorMsg.getCode().toString());
            case BAD_REQUEST:
                OFBadRequestErrorMsg ofBadRequestErrorMsg = (OFBadRequestErrorMsg)errorMsg;
                return String.format("Error %s %s",
                        errorMsg.getErrType().toString(),
                        ofBadRequestErrorMsg.getCode().toString());
            case BAD_ACTION:
                OFBadActionErrorMsg ofBadActionErrorMsg = (OFBadActionErrorMsg)errorMsg;
                return String.format("Error %s %s",
                        errorMsg.getErrType().toString(),
                        ofBadActionErrorMsg.getCode().toString());
            case FLOW_MOD_FAILED:
                OFFlowModFailedErrorMsg ofFlowModFailedErrorMsg = (OFFlowModFailedErrorMsg)errorMsg;
                return String.format("Error %s %s",
                        errorMsg.getErrType().toString(),
                        ofFlowModFailedErrorMsg.getCode().toString());
            case PORT_MOD_FAILED:
                OFPortModFailedErrorMsg ofPortModFailedErrorMsg = (OFPortModFailedErrorMsg)errorMsg;
                return String.format("Error %s %s",
                        errorMsg.getErrType().toString(),
                        ofPortModFailedErrorMsg.getCode().toString());
            case QUEUE_OP_FAILED:
                OFQueueOpFailedErrorMsg ofQueueOpFailedErrorMsg = (OFQueueOpFailedErrorMsg)errorMsg;
                return String.format("Error %s %s",
                        errorMsg.getErrType().toString(),
                        ofQueueOpFailedErrorMsg.getCode().toString());
            case EXPERIMENTER:
                // no codes known for vendor error
                return String.format("Error %s", errorMsg.getErrType().toString());
            default:
                break;
        }
        return null;
    }
}
