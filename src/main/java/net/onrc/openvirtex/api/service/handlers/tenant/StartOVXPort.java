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
 ******************************************************************************/
package net.onrc.openvirtex.api.service.handlers.tenant;

import java.util.HashMap;
import java.util.Map;

import net.onrc.openvirtex.api.service.handlers.ApiHandler;
import net.onrc.openvirtex.api.service.handlers.HandlerUtils;
import net.onrc.openvirtex.api.service.handlers.TenantHandler;
import net.onrc.openvirtex.elements.OVXMap;
import net.onrc.openvirtex.elements.network.OVXNetwork;
import net.onrc.openvirtex.elements.port.OVXPort;
import net.onrc.openvirtex.exceptions.InvalidDPIDException;
import net.onrc.openvirtex.exceptions.InvalidPortException;
import net.onrc.openvirtex.exceptions.InvalidTenantIdException;
import net.onrc.openvirtex.exceptions.MissingRequiredField;
import net.onrc.openvirtex.exceptions.NetworkMappingException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.thetransactioncompany.jsonrpc2.JSONRPC2Error;
import com.thetransactioncompany.jsonrpc2.JSONRPC2ParamsType;
import com.thetransactioncompany.jsonrpc2.JSONRPC2Response;

public class StartOVXPort extends ApiHandler<Map<String, Object>> {
    Logger log = LogManager.getLogger(StartOVXPort.class.getName());

    @Override
    public JSONRPC2Response process(final Map<String, Object> params) {
        JSONRPC2Response resp = null;

        try {
            final Number tenantId = HandlerUtils.<Number>fetchField(
                    TenantHandler.TENANT, params, true, null);
            final Number dpid = HandlerUtils.<Number>fetchField(
                    TenantHandler.VDPID, params, true, null);
            final Number port = HandlerUtils.<Number>fetchField(
                    TenantHandler.VPORT, params, true, null);


            HandlerUtils.isValidTenantId(tenantId.intValue());
            HandlerUtils
                    .isValidOVXSwitch(tenantId.intValue(), dpid.longValue());
            HandlerUtils.isValidOVXPort(tenantId.intValue(), dpid.longValue(),
                    port.shortValue());


            final OVXMap map = OVXMap.getInstance();
            final OVXNetwork virtualNetwork = map.getVirtualNetwork(tenantId
                    .intValue());

            virtualNetwork.startPort(dpid.longValue(), port.shortValue());

            this.log.info(
                    "Start virtual port {} on virtual switch {} in virtual network {}",
                    port, dpid, virtualNetwork.getTenantId());
            OVXPort ovxPort = virtualNetwork.getSwitch(dpid.longValue())
                    .getPort(port.shortValue());
            Map<String, Object> reply = new HashMap<String, Object>(
                    ovxPort.getDBObject());
            reply.put(TenantHandler.VDPID, ovxPort.getParentSwitch()
                    .getSwitchId());
            reply.put(TenantHandler.TENANT, ovxPort.getTenantId());
            resp = new JSONRPC2Response(reply, 0);

        } catch (final MissingRequiredField e) {
            resp = new JSONRPC2Response(
                    new JSONRPC2Error(
                            JSONRPC2Error.INVALID_PARAMS.getCode(),
                            this.cmdName()
                                    + ": Unable to delete this virtual port in the virtual network : "
                                    + e.getMessage()), 0);
        } catch (final InvalidPortException e) {
            resp = new JSONRPC2Response(new JSONRPC2Error(
                    JSONRPC2Error.INVALID_PARAMS.getCode(), this.cmdName()
                    + ": Invalid port : " + e.getMessage()), 0);
        } catch (final InvalidTenantIdException e) {
            resp = new JSONRPC2Response(new JSONRPC2Error(
                    JSONRPC2Error.INVALID_PARAMS.getCode(), this.cmdName()
                    + ": Invalid tenant id : " + e.getMessage()), 0);
        } catch (final InvalidDPIDException e) {
            resp = new JSONRPC2Response(new JSONRPC2Error(
                    JSONRPC2Error.INVALID_PARAMS.getCode(), this.cmdName()
                    + ": Invalid virtual dpid : " + e.getMessage()), 0);
        } catch (final NetworkMappingException e) {
            resp = new JSONRPC2Response(new JSONRPC2Error(
                    JSONRPC2Error.INVALID_PARAMS.getCode(), this.cmdName()
                    + ": " + e.getMessage()), 0);
        }
        return resp;
    }


    @Override
    public JSONRPC2ParamsType getType() {
        return JSONRPC2ParamsType.OBJECT;
    }

}
