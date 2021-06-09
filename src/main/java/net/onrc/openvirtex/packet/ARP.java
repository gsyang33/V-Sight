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
import java.util.Arrays;

/**
 *
 * @author David Erickson (daviderickson@cs.stanford.edu)
 */
public class ARP extends BasePacket {
    public static final short HW_TYPE_ETHERNET = 0x1;

    public static final short PROTO_TYPE_IP = 0x800;

    public static final short OP_REQUEST = 0x1;
    public static final short OP_REPLY = 0x2;
    public static final short OP_RARP_REQUEST = 0x3;
    public static final short OP_RARP_REPLY = 0x4;

    protected short hardwareType;
    protected short protocolType;
    protected byte hardwareAddressLength;
    protected byte protocolAddressLength;
    protected short opCode;
    protected byte[] senderHardwareAddress;
    protected byte[] senderProtocolAddress;
    protected byte[] targetHardwareAddress;
    protected byte[] targetProtocolAddress;

    /**
     * @return the hardwareType
     */
    public short getHardwareType() {
        return this.hardwareType;
    }

    /**
     * @param hardwareType
     *            the hardwareType to set
     */
    public ARP setHardwareType(final short hardwareType) {
        this.hardwareType = hardwareType;
        return this;
    }

    /**
     * @return the protocolType
     */
    public short getProtocolType() {
        return this.protocolType;
    }

    /**
     * @param protocolType
     *            the protocolType to set
     */
    public ARP setProtocolType(final short protocolType) {
        this.protocolType = protocolType;
        return this;
    }

    /**
     * @return the hardwareAddressLength
     */
    public byte getHardwareAddressLength() {
        return this.hardwareAddressLength;
    }

    /**
     * @param hardwareAddressLength
     *            the hardwareAddressLength to set
     */
    public ARP setHardwareAddressLength(final byte hardwareAddressLength) {
        this.hardwareAddressLength = hardwareAddressLength;
        return this;
    }

    /**
     * @return the protocolAddressLength
     */
    public byte getProtocolAddressLength() {
        return this.protocolAddressLength;
    }

    /**
     * @param protocolAddressLength
     *            the protocolAddressLength to set
     */
    public ARP setProtocolAddressLength(final byte protocolAddressLength) {
        this.protocolAddressLength = protocolAddressLength;
        return this;
    }

    /**
     * @return the opCode
     */
    public short getOpCode() {
        return this.opCode;
    }

    /**
     * @param opCode
     *            the opCode to set
     */
    public ARP setOpCode(final short opCode) {
        this.opCode = opCode;
        return this;
    }

    /**
     * @return the senderHardwareAddress
     */
    public byte[] getSenderHardwareAddress() {
        return this.senderHardwareAddress;
    }

    /**
     * @param senderHardwareAddress
     *            the senderHardwareAddress to set
     */
    public ARP setSenderHardwareAddress(final byte[] senderHardwareAddress) {
        this.senderHardwareAddress = senderHardwareAddress;
        return this;
    }

    /**
     * @return the senderProtocolAddress
     */
    public byte[] getSenderProtocolAddress() {
        return this.senderProtocolAddress;
    }

    /**
     * @param senderProtocolAddress
     *            the senderProtocolAddress to set
     */
    public ARP setSenderProtocolAddress(final byte[] senderProtocolAddress) {
        this.senderProtocolAddress = senderProtocolAddress;
        return this;
    }

    public ARP setSenderProtocolAddress(final int address) {
        this.senderProtocolAddress = ByteBuffer.allocate(4).putInt(address)
                .array();
        return this;
    }

    /**
     * @return the targetHardwareAddress
     */
    public byte[] getTargetHardwareAddress() {
        return this.targetHardwareAddress;
    }

    /**
     * @param targetHardwareAddress
     *            the targetHardwareAddress to set
     */
    public ARP setTargetHardwareAddress(final byte[] targetHardwareAddress) {
        this.targetHardwareAddress = targetHardwareAddress;
        return this;
    }

    /**
     * @return the targetProtocolAddress
     */
    public byte[] getTargetProtocolAddress() {
        return this.targetProtocolAddress;
    }

    /**
     * @return True if gratuitous ARP (SPA = TPA), false otherwise
     */
    public boolean isGratuitous() {
        assert this.senderProtocolAddress.length == this.targetProtocolAddress.length;

        int indx = 0;
        while (indx < this.senderProtocolAddress.length) {
            if (this.senderProtocolAddress[indx] != this.targetProtocolAddress[indx]) {
                return false;
            }
            indx++;
        }

        return true;
    }

    /**
     * @param targetProtocolAddress
     *            the targetProtocolAddress to set
     */
    public ARP setTargetProtocolAddress(final byte[] targetProtocolAddress) {
        this.targetProtocolAddress = targetProtocolAddress;
        return this;
    }

    public ARP setTargetProtocolAddress(final int address) {
        this.targetProtocolAddress = ByteBuffer.allocate(4).putInt(address)
                .array();
        return this;
    }

    @Override
    public byte[] serialize() {
        final int length = 8 + 2 * (0xff & this.hardwareAddressLength) + 2
                * (0xff & this.protocolAddressLength);
        final byte[] data = new byte[length];
        final ByteBuffer bb = ByteBuffer.wrap(data);
        bb.putShort(this.hardwareType);
        bb.putShort(this.protocolType);
        bb.put(this.hardwareAddressLength);
        bb.put(this.protocolAddressLength);
        bb.putShort(this.opCode);
        bb.put(this.senderHardwareAddress, 0, 0xff & this.hardwareAddressLength);
        bb.put(this.senderProtocolAddress, 0, 0xff & this.protocolAddressLength);
        bb.put(this.targetHardwareAddress, 0, 0xff & this.hardwareAddressLength);
        bb.put(this.targetProtocolAddress, 0, 0xff & this.protocolAddressLength);
        return data;
    }

    @Override
    public IPacket deserialize(final byte[] data, final int offset,
            final int length) {
        final ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
        this.hardwareType = bb.getShort();
        this.protocolType = bb.getShort();
        this.hardwareAddressLength = bb.get();
        this.protocolAddressLength = bb.get();
        this.opCode = bb.getShort();
        this.senderHardwareAddress = new byte[0xff & this.hardwareAddressLength];
        bb.get(this.senderHardwareAddress, 0, this.senderHardwareAddress.length);
        this.senderProtocolAddress = new byte[0xff & this.protocolAddressLength];
        bb.get(this.senderProtocolAddress, 0, this.senderProtocolAddress.length);
        this.targetHardwareAddress = new byte[0xff & this.hardwareAddressLength];
        bb.get(this.targetHardwareAddress, 0, this.targetHardwareAddress.length);
        this.targetProtocolAddress = new byte[0xff & this.protocolAddressLength];
        bb.get(this.targetProtocolAddress, 0, this.targetProtocolAddress.length);
        return this;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 13121;
        int result = super.hashCode();
        result = prime * result + this.hardwareAddressLength;
        result = prime * result + this.hardwareType;
        result = prime * result + this.opCode;
        result = prime * result + this.protocolAddressLength;
        result = prime * result + this.protocolType;
        result = prime * result + Arrays.hashCode(this.senderHardwareAddress);
        result = prime * result + Arrays.hashCode(this.senderProtocolAddress);
        result = prime * result + Arrays.hashCode(this.targetHardwareAddress);
        result = prime * result + Arrays.hashCode(this.targetProtocolAddress);
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
        if (!(obj instanceof ARP)) {
            return false;
        }
        final ARP other = (ARP) obj;
        if (this.hardwareAddressLength != other.hardwareAddressLength) {
            return false;
        }
        if (this.hardwareType != other.hardwareType) {
            return false;
        }
        if (this.opCode != other.opCode) {
            return false;
        }
        if (this.protocolAddressLength != other.protocolAddressLength) {
            return false;
        }
        if (this.protocolType != other.protocolType) {
            return false;
        }
        if (!Arrays.equals(this.senderHardwareAddress,
                other.senderHardwareAddress)) {
            return false;
        }
        if (!Arrays.equals(this.senderProtocolAddress,
                other.senderProtocolAddress)) {
            return false;
        }
        if (!Arrays.equals(this.targetHardwareAddress,
                other.targetHardwareAddress)) {
            return false;
        }
        if (!Arrays.equals(this.targetProtocolAddress,
                other.targetProtocolAddress)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ARP [hardwareType=" + this.hardwareType + ", protocolType="
                + this.protocolType + ", hardwareAddressLength="
                + this.hardwareAddressLength + ", protocolAddressLength="
                + this.protocolAddressLength + ", opCode=" + this.opCode
                + ", senderHardwareAddress="
                + Arrays.toString(this.senderHardwareAddress)
                + ", senderProtocolAddress="
                + Arrays.toString(this.senderProtocolAddress)
                + ", targetHardwareAddress="
                + Arrays.toString(this.targetHardwareAddress)
                + ", targetProtocolAddress="
                + Arrays.toString(this.targetProtocolAddress) + "]";
    }
}
