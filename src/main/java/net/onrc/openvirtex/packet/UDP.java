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
/**
 * Copyright 2011, Big Switch Networks, Inc.
 * Originally created by David Erickson, Stanford University
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 **/

package net.onrc.openvirtex.packet;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author David Erickson (daviderickson@cs.stanford.edu)
 */

public class UDP extends BasePacket {
    public static Map<Short, Class<? extends IPacket>> decodeMap;
    public static final short DHCP_SERVER_PORT = (short) 67;
    public static final short DHCP_CLIENT_PORT = (short) 68;

    static {
        UDP.decodeMap = new HashMap<Short, Class<? extends IPacket>>();
        /*
         * Disable DHCP until the deserialize code is hardened to deal with
         * garbage input
         */
        UDP.decodeMap.put(UDP.DHCP_SERVER_PORT, DHCP.class);
        UDP.decodeMap.put(UDP.DHCP_CLIENT_PORT, DHCP.class);

    }


    protected short sourcePort;
    protected short destinationPort;
    protected short length;
    protected short checksum;

    /**
     * @return the sourcePort
     */
    public short getSourcePort() {
        return this.sourcePort;
    }

    /**
     * @param sourcePort
     *            the sourcePort to set
     */
    public UDP setSourcePort(final short sourcePort) {
        this.sourcePort = sourcePort;
        return this;
    }

    /**
     * @return the destinationPort
     */
    public short getDestinationPort() {
        return this.destinationPort;
    }

    /**
     * @param destinationPort
     *            the destinationPort to set
     */
    public UDP setDestinationPort(final short destinationPort) {
        this.destinationPort = destinationPort;
        return this;
    }

    /**
     * @return the length
     */
    public short getLength() {
        return this.length;
    }

    /**
     * @return the checksum
     */
    public short getChecksum() {
        return this.checksum;
    }

    /**
     * @param checksum
     *            the checksum to set
     */
    public UDP setChecksum(final short checksum) {
        this.checksum = checksum;
        return this;
    }

    @Override
    public void resetChecksum() {
        this.checksum = 0;
        super.resetChecksum();
    }

    /**
     * Serializes the packet. Will compute and set the following fields if they
     * are set to specific values at the time serialize is called: -checksum : 0
     * -length : 0
     */
    @Override
    public byte[] serialize() {
        byte[] payloadData = null;
        if (this.payload != null) {
            this.payload.setParent(this);
            payloadData = this.payload.serialize();
        }

        this.length = (short) (8 + (payloadData == null ? 0
                : payloadData.length));

        final byte[] data = new byte[this.length];
        final ByteBuffer bb = ByteBuffer.wrap(data);


        bb.putShort(this.sourcePort);
        bb.putShort(this.destinationPort);
        bb.putShort(this.length);
        bb.putShort(this.checksum);
        if (payloadData != null) {
            bb.put(payloadData);
        }

        if (this.parent != null && this.parent instanceof IPv4) {
            ((IPv4) this.parent).setProtocol(IPv4.PROTOCOL_UDP);
        }

        // compute checksum if needed
        if (this.checksum == 0) {
            bb.rewind();
            int accumulation = 0;

            // compute pseudo header mac
            if (this.parent != null && this.parent instanceof IPv4) {
                final IPv4 ipv4 = (IPv4) this.parent;
                accumulation += (ipv4.getSourceAddress() >> 16 & 0xffff)
                        + (ipv4.getSourceAddress() & 0xffff);
                accumulation += (ipv4.getDestinationAddress() >> 16 & 0xffff)
                        + (ipv4.getDestinationAddress() & 0xffff);
                accumulation += ipv4.getProtocol() & 0xff;
                accumulation += this.length & 0xffff;
            }

            for (int i = 0; i < this.length / 2; ++i) {
                accumulation += 0xffff & bb.getShort();
            }
            // pad to an even number of shorts
            if (this.length % 2 > 0) {
                accumulation += (bb.get() & 0xff) << 8;
            }

            accumulation = (accumulation >> 16 & 0xffff)
                    + (accumulation & 0xffff);
            this.checksum = (short) (~accumulation & 0xffff);
            bb.putShort(6, this.checksum);
        }
        return data;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 5807;
        int result = super.hashCode();
        result = prime * result + this.checksum;
        result = prime * result + this.destinationPort;
        result = prime * result + this.length;
        result = prime * result + this.sourcePort;
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof UDP)) {
            return false;
        }
        final UDP other = (UDP) obj;
        if (this.checksum != other.checksum) {
            return false;
        }
        if (this.destinationPort != other.destinationPort) {
            return false;
        }
        if (this.length != other.length) {
            return false;
        }
        if (this.sourcePort != other.sourcePort) {
            return false;
        }
        return true;
    }

    @Override
    public IPacket deserialize(final byte[] data, final int offset,
            final int length) {
        final ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
        this.sourcePort = bb.getShort();
        this.destinationPort = bb.getShort();
        this.length = bb.getShort();
        this.checksum = bb.getShort();


        if (UDP.decodeMap.containsKey(this.destinationPort)) {
            try {
                this.payload = UDP.decodeMap.get(this.destinationPort)
                        .getConstructor().newInstance();
            } catch (final Exception e) {
                throw new RuntimeException("Failure instantiating class", e);
            }
        } else if (UDP.decodeMap.containsKey(this.sourcePort)) {
            try {
                this.payload = UDP.decodeMap.get(this.sourcePort)
                        .getConstructor().newInstance();
            } catch (final Exception e) {
                throw new RuntimeException("Failure instantiating class", e);
            }
        } else {
            this.payload = new Data();
        }
        this.payload = this.payload.deserialize(data, bb.position(), bb.limit()
                - bb.position());
        this.payload.setParent(this);
        return this;
    }
}
