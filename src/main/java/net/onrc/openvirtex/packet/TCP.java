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

/**
 *
 * @author shudong.zhou@bigswitch.com
 */

public class TCP extends BasePacket {
    protected short sourcePort;
    protected short destinationPort;
    protected int sequence;
    protected int acknowledge;
    protected byte dataOffset;
    protected short flags;
    protected short windowSize;
    protected short checksum;
    protected short urgentPointer;
    protected byte[] options;

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
    public TCP setSourcePort(final short sourcePort) {
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
    public TCP setDestinationPort(final short destinationPort) {
        this.destinationPort = destinationPort;
        return this;
    }

    /**
     * @return the checksum
     */
    public short getChecksum() {
        return this.checksum;
    }

    public int getSequence() {
        return this.sequence;
    }

    public TCP setSequence(final int seq) {
        this.sequence = seq;
        return this;
    }

    public int getAcknowledge() {
        return this.acknowledge;
    }

    public TCP setAcknowledge(final int ack) {
        this.acknowledge = ack;
        return this;
    }

    public byte getDataOffset() {
        return this.dataOffset;
    }

    public TCP setDataOffset(final byte offset) {
        this.dataOffset = offset;
        return this;
    }

    public short getFlags() {
        return this.flags;
    }

    public TCP setFlags(final short flags) {
        this.flags = flags;
        return this;
    }

    public short getWindowSize() {
        return this.windowSize;
    }

    public TCP setWindowSize(final short windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public short getTcpChecksum() {
        return this.checksum;
    }

    public TCP setTcpChecksum(final short checksum) {
        this.checksum = checksum;
        return this;
    }

    @Override
    public void resetChecksum() {
        this.checksum = 0;
        super.resetChecksum();
    }

    public short getUrgentPointer(final short urgentPointer) {
        return this.urgentPointer;
    }

    public TCP setUrgentPointer(final short urgentPointer) {
        this.urgentPointer = urgentPointer;
        return this;
    }

    public byte[] getOptions() {
        return this.options;
    }

    public TCP setOptions(final byte[] options) {
        this.options = options;
        this.dataOffset = (byte) (20 + options.length + 3 >> 2);
        return this;
    }

    /**
     * @param checksum
     *            the checksum to set
     */
    public TCP setChecksum(final short checksum) {
        this.checksum = checksum;
        return this;
    }

    /**
     * Serializes the packet. Will compute and set the following fields if they
     * are set to specific values at the time serialize is called: -checksum : 0
     * -length : 0
     */
    @Override
    public byte[] serialize() {
        int length;
        if (this.dataOffset == 0) {
            this.dataOffset = 5; // default header length
        }
        length = this.dataOffset << 2;
        byte[] payloadData = null;
        if (this.payload != null) {
            this.payload.setParent(this);
            payloadData = this.payload.serialize();
            length += payloadData.length;
        }

        final byte[] data = new byte[length];
        final ByteBuffer bb = ByteBuffer.wrap(data);

        bb.putShort(this.sourcePort);
        bb.putShort(this.destinationPort);
        bb.putInt(this.sequence);
        bb.putInt(this.acknowledge);
        bb.putShort((short) (this.flags | this.dataOffset << 12));
        bb.putShort(this.windowSize);
        bb.putShort(this.checksum);
        bb.putShort(this.urgentPointer);
        if (this.dataOffset > 5) {
            int padding;
            bb.put(this.options);
            padding = (this.dataOffset << 2) - 20 - this.options.length;
            for (int i = 0; i < padding; i++) {
                bb.put((byte) 0);
            }
        }
        if (payloadData != null) {
            bb.put(payloadData);
        }

        if (this.parent != null && this.parent instanceof IPv4) {
            ((IPv4) this.parent).setProtocol(IPv4.PROTOCOL_TCP);
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
                accumulation += length & 0xffff;
            }

            for (int i = 0; i < length / 2; ++i) {
                accumulation += 0xffff & bb.getShort();
            }
            // pad to an even number of shorts
            if (length % 2 > 0) {
                accumulation += (bb.get() & 0xff) << 8;
            }

            accumulation = (accumulation >> 16 & 0xffff)
                    + (accumulation & 0xffff);
            this.checksum = (short) (~accumulation & 0xffff);
            bb.putShort(16, this.checksum);
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
        if (!(obj instanceof TCP)) {
            return false;
        }
        final TCP other = (TCP) obj;
        // May want to compare fields based on the flags set
        return this.checksum == other.checksum
                && this.destinationPort == other.destinationPort
                && this.sourcePort == other.sourcePort
                && this.sequence == other.sequence
                && this.acknowledge == other.acknowledge
                && this.dataOffset == other.dataOffset
                && this.flags == other.flags
                && this.windowSize == other.windowSize
                && this.urgentPointer == other.urgentPointer
                && (this.dataOffset == 5 || this.options.equals(other.options));
    }

    @Override
    public IPacket deserialize(final byte[] data, final int offset,
            final int length) {
        final ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
        this.sourcePort = bb.getShort();
        this.destinationPort = bb.getShort();
        this.sequence = bb.getInt();
        this.acknowledge = bb.getInt();
        this.flags = bb.getShort();
        this.dataOffset = (byte) (this.flags >> 12 & 0xf);
        this.flags = (short) (this.flags & 0x1ff);
        this.windowSize = bb.getShort();
        this.checksum = bb.getShort();
        this.urgentPointer = bb.getShort();
        if (this.dataOffset > 5) {
            int optLength = (this.dataOffset << 2) - 20;
            if (bb.limit() < bb.position() + optLength) {
                optLength = bb.limit() - bb.position();
            }
            try {
                this.options = new byte[optLength];
                bb.get(this.options, 0, optLength);
            } catch (final IndexOutOfBoundsException e) {
                this.options = null;
            }
        }

        this.payload = new Data();
        this.payload = this.payload.deserialize(data, bb.position(), bb.limit()
                - bb.position());
        this.payload.setParent(this);
        return this;
    }
}
