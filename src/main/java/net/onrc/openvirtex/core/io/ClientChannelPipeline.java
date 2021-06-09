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
package net.onrc.openvirtex.core.io;

import java.util.concurrent.ThreadPoolExecutor;

import net.onrc.openvirtex.core.OpenVirteXController;
import net.onrc.openvirtex.elements.datapath.OVXSwitch;
import net.onrc.openvirtex.elements.network.PhysicalNetwork;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;

public class ClientChannelPipeline extends OpenflowChannelPipeline {

    Logger log = LogManager.getLogger(ClientChannelPipeline.class.getName());

    private ClientBootstrap bootstrap = null;
    private OVXSwitch sw = null;
    private final ChannelGroup cg;

    public ClientChannelPipeline(
            final OpenVirteXController openVirteXController,
            final ChannelGroup cg, final ThreadPoolExecutor pipelineExecutor,
            final ClientBootstrap bootstrap, final OVXSwitch sw) {
        super();
        this.ctrl = openVirteXController;
        this.pipelineExecutor = pipelineExecutor;
        this.timer = PhysicalNetwork.getTimer();
        this.idleHandler = new IdleStateHandler(this.timer, 40, 50, 0);
        // ksyang: modified the timeout Seconds into 90 to prohibit the removal of vSwitches (original: 30)
        this.readTimeoutHandler = new ReadTimeoutHandler(this.timer, 30);
        this.bootstrap = bootstrap;
        this.sw = sw;
        this.cg = cg;

        //this.log.info("[ksyang] Clientchannel created");
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        final ControllerChannelHandler handler = new ControllerChannelHandler(
                this.ctrl, this.sw);

        final ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("reconnect", new ReconnectHandler(this.sw,
                this.bootstrap, this.timer, 15, this.cg));
        pipeline.addLast("ofmessagedecoder", new OVXMessageDecoder());
        pipeline.addLast("ofmessageencoder", new OVXMessageEncoder());
        pipeline.addLast("idle", this.idleHandler);
        pipeline.addLast("timeout", this.readTimeoutHandler);
        pipeline.addLast("handshaketimeout", new HandshakeTimeoutHandler(
                handler, this.timer, 15));

        pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                this.pipelineExecutor));
        pipeline.addLast("handler", handler);
        return pipeline;
    }

}
