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

/**
 *
 */
package net.onrc.openvirtex.packet;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author David Erickson (daviderickson@cs.stanford.edu)
 *
 */
public class IPv4 extends BasePacket {
    public static final byte PROTOCOL_ICMP = 0x1;
    public static final byte PROTOCOL_TCP = 0x6;
    public static final byte PROTOCOL_UDP = 0x11;
    public static Map<Byte, Class<? extends IPacket>> protocolClassMap;

    static {
        IPv4.protocolClassMap = new HashMap<Byte, Class<? extends IPacket>>();
        IPv4.protocolClassMap.put(IPv4.PROTOCOL_ICMP, ICMP.class);
        IPv4.protocolClassMap.put(IPv4.PROTOCOL_TCP, TCP.class);
        IPv4.protocolClassMap.put(IPv4.PROTOCOL_UDP, UDP.class);
    }

    protected byte version;
    protected byte headerLength;
    protected byte diffServ;
    protected short totalLength;
    protected short identification;
    protected byte flags;
    protected short fragmentOffset;
    protected byte ttl;
    protected byte protocol;
    protected short checksum;
    protected int sourceAddress;
    protected int destinationAddress;
    protected byte[] options;

    protected boolean isTruncated;

    protected int trimLength;

    /**
     * Default constructor that sets the version to 4.
     */
    public IPv4() {
        super();
        this.version = 4;
        this.isTruncated = false;
    }

    /**
     * @return the version
     */
    public byte getVersion() {
        return this.version;
    }

    /**
     * @param version
     *            the version to set
     */
    public IPv4 setVersion(final byte version) {
        this.version = version;
        return this;
    }

    /**
     * @return the headerLength
     */
    public byte getHeaderLength() {
        return this.headerLength;
    }

    /**
     * @return the diffServ
     */
    public byte getDiffServ() {
        return this.diffServ;
    }

    /**
     * @param diffServ
     *            the diffServ to set
     */
    public IPv4 setDiffServ(final byte diffServ) {
        this.diffServ = diffServ;
        return this;
    }

    /**
     * @return the totalLength
     */
    public short getTotalLength() {
        return this.totalLength;
    }

    public int getTrimLength() {
        return this.trimLength;
    }

    /**
     * @return the identification
     */
    public short getIdentification() {
        return this.identification;
    }

    public boolean isTruncated() {
        return this.isTruncated;
    }

    public void setTruncated(final boolean isTruncated) {
        this.isTruncated = isTruncated;
    }

    /**
     * @param identification
     *            the identification to set
     */
    public IPv4 setIdentification(final short identification) {
        this.identification = identification;
        return this;
    }

    /**
     * @return the flags
     */
    public byte getFlags() {
        return this.flags;
    }

    /**
     * @param flags
     *            the flags to set
     */
    public IPv4 setFlags(final byte flags) {
        this.flags = flags;
        return this;
    }

    /**
     * @return the fragmentOffset
     */
    public short getFragmentOffset() {
        return this.fragmentOffset;
    }

    /**
     * @param fragmentOffset
     *            the fragmentOffset to set
     */
    public IPv4 setFragmentOffset(final short fragmentOffset) {
        this.fragmentOffset = fragmentOffset;
        return this;
    }

    /**
     * @return the ttl
     */
    public byte getTtl() {
        return this.ttl;
    }

    /**
     * @param ttl
     *            the ttl to set
     */
    public IPv4 setTtl(final byte ttl) {
        this.ttl = ttl;
        return this;
    }

    /**
     * @return the protocol
     */
    public byte getProtocol() {
        return this.protocol;
    }

    /**
     * @param protocol
     *            the protocol to set
     */
    public IPv4 setProtocol(final byte protocol) {
        this.protocol = protocol;
        return this;
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
    public IPv4 setChecksum(final short checksum) {
        this.checksum = checksum;
        return this;
    }

    @Override
    public void resetChecksum() {
        this.checksum = 0;
        super.resetChecksum();
    }

    /**
     * @return the sourceAddress
     */
    public int getSourceAddress() {
        return this.sourceAddress;
    }

    /**
     * @param sourceAddress
     *            the sourceAddress to set
     */
    public IPv4 setSourceAddress(final int sourceAddress) {
        this.sourceAddress = sourceAddress;
        return this;
    }

    /**
     * @param sourceAddress
     *            the sourceAddress to set
     */
    public IPv4 setSourceAddress(final String sourceAddress) {
        this.sourceAddress = IPv4.toIPv4Address(sourceAddress);
        return this;
    }

    /**
     * @return the destinationAddress
     */
    public int getDestinationAddress() {
        return this.destinationAddress;
    }

    /**
     * @param destinationAddress
     *            the destinationAddress to set
     */
    public IPv4 setDestinationAddress(final int destinationAddress) {
        this.destinationAddress = destinationAddress;
        return this;
    }

    /**
     * @param destinationAddress
     *            the destinationAddress to set
     */
    public IPv4 setDestinationAddress(final String destinationAddress) {
        this.destinationAddress = IPv4.toIPv4Address(destinationAddress);
        return this;
    }

    /**
     * @return the options
     */
    public byte[] getOptions() {
        return this.options;
    }

    /**
     * @param options
     *            the options to set
     */
    public IPv4 setOptions(final byte[] options) {
        if (options != null && options.length % 4 > 0) {
            throw new IllegalArgumentException(
                    "Options length must be a multiple of 4");
        }
        this.options = options;
        return this;
    }

    /**
     * Serializes the packet. Will compute and set the following fields if they
     * are set to specific values at the time serialize is called: -checksum : 0
     * -headerLength : 0 -totalLength : 0
     */
    @Override
    public byte[] serialize() {
        byte[] payloadData = null;
        if (this.payload != null) {
            this.payload.setParent(this);
            payloadData = this.payload.serialize();
        }

        int optionsLength = 0;
        if (this.options != null) {
            optionsLength = this.options.length / 4;
        }
        this.headerLength = (byte) (5 + optionsLength);

        this.totalLength = (short) (this.headerLength * 4 + (payloadData == null ? 0
                : payloadData.length));

        final byte[] data = new byte[this.totalLength];
        final ByteBuffer bb = ByteBuffer.wrap(data);

        bb.put((byte) ((this.version & 0xf) << 4 | this.headerLength & 0xf));
        bb.put(this.diffServ);
        bb.putShort(this.totalLength);
        bb.putShort(this.identification);
        bb.putShort((short) ((this.flags & 0x7) << 13 | this.fragmentOffset & 0x1fff));
        bb.put(this.ttl);
        bb.put(this.protocol);
        bb.putShort(this.checksum);
        bb.putInt(this.sourceAddress);
        bb.putInt(this.destinationAddress);
        if (this.options != null) {
            bb.put(this.options);
        }
        if (payloadData != null) {
            bb.put(payloadData);
        }

        // compute checksum if needed
        if (this.checksum == 0) {
            bb.rewind();
            int accumulation = 0;
            for (int i = 0; i < this.headerLength * 2; ++i) {
                accumulation += 0xffff & bb.getShort();
            }
            accumulation = (accumulation >> 16 & 0xffff)
                    + (accumulation & 0xffff);
            this.checksum = (short) (~accumulation & 0xffff);
            bb.putShort(10, this.checksum);
        }
        return data;
    }

    @Override
    public IPacket deserialize(final byte[] data, final int offset,
            final int length) {
        final ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
        short sscratch;

        this.version = bb.get();
        this.headerLength = (byte) (this.version & 0xf);
        this.version = (byte) (this.version >> 4 & 0xf);
        this.diffServ = bb.get();
        this.totalLength = bb.getShort();
        this.identification = bb.getShort();
        sscratch = bb.getShort();
        this.flags = (byte) (sscratch >> 13 & 0x7);
        this.fragmentOffset = (short) (sscratch & 0x1fff);
        this.ttl = bb.get();
        this.protocol = bb.get();
        this.checksum = bb.getShort();
        this.sourceAddress = bb.getInt();
        this.destinationAddress = bb.getInt();

        if (this.headerLength > 5) {
            final int optionsLength = (this.headerLength - 5) * 4;
            this.options = new byte[optionsLength];
            bb.get(this.options);
        }

        IPacket payload;
        if (IPv4.protocolClassMap.containsKey(this.protocol)) {
            final Class<? extends IPacket> clazz = IPv4.protocolClassMap
                    .get(this.protocol);
            try {
                payload = clazz.newInstance();
            } catch (final Exception e) {
                throw new RuntimeException(
                        "Error parsing payload for IPv4 packet", e);
            }
        } else {
            payload = new Data();
        }
        this.payload = payload.deserialize(data, bb.position(),
                bb.limit() - bb.position());
        this.payload.setParent(this);

        //for trim data in packet_in for OF_1.3
        if (this.totalLength != length) {
            this.isTruncated = true;
            this.trimLength = length - this.totalLength;
        } else {
            this.isTruncated = false;
        }

        return this;
    }

    /**
     * Accepts an IPv4 address of the form xxx.xxx.xxx.xxx, ie 192.168.0.1 and
     * returns the corresponding 32 bit integer.
     *
     * @param ipAddress
     * @return
     */
    public static int toIPv4Address(final String ipAddress) {
        if (ipAddress == null) {
            throw new IllegalArgumentException("Specified IPv4 address must"
                    + "contain 4 sets of numerical digits separated by periods");
        }
        final String[] octets = ipAddress.split("\\.");
        if (octets.length != 4) {
            throw new IllegalArgumentException("Specified IPv4 address must"
                    + "contain 4 sets of numerical digits separated by periods");
        }

        int result = 0;
        for (int i = 0; i < 4; ++i) {
            result |= Integer.valueOf(octets[i]) << (3 - i) * 8;
        }
        return result;
    }

    /**
     * Accepts an IPv4 address in a byte array and returns the corresponding
     * 32-bit integer value.
     *
     * @param ipAddress
     * @return
     */
    public static int toIPv4Address(final byte[] ipAddress) {
        int ip = 0;
        for (int i = 0; i < 4; i++) {
            final int t = (ipAddress[i] & 0xff) << (3 - i) * 8;
            ip |= t;
        }
        return ip;
    }

    /**
     * Accepts an IPv4 address and returns of string of the form xxx.xxx.xxx.xxx,
     * e.g., 192.168.0.1.
     *
     * @param ipAddress
     * @return
     */
    public static String fromIPv4Address(final int ipAddress) {
        final StringBuffer sb = new StringBuffer();
        int result = 0;
        for (int i = 0; i < 4; ++i) {
            result = ipAddress >> (3 - i) * 8 & 0xff;
            sb.append(Integer.valueOf(result).toString());
            if (i != 3) {
                sb.append(".");
            }
        }
        return sb.toString();
    }

    /**
     * Accepts a collection of IPv4 addresses as integers and returns a single
     * String useful in toString method's containing collections of IP
     * addresses.
     *
     * @param ipAddresses
     *            collection
     * @return
     */
    public static String fromIPv4AddressCollection(
            final Collection<Integer> ipAddresses) {
        if (ipAddresses == null) {
            return "null";
        }
        final StringBuffer sb = new StringBuffer();
        sb.append("[");
        for (final Integer ip : ipAddresses) {
            sb.append(IPv4.fromIPv4Address(ip));
            sb.append(",");
        }
        sb.replace(sb.length() - 1, sb.length(), "]");
        return sb.toString();
    }

    /**
     * Accepts an IPv4 address of the form xxx.xxx.xxx.xxx, ie 192.168.0.1 and
     * returns the corresponding byte array.
     *
     * @param ipAddress
     *            The IP address in the form xx.xxx.xxx.xxx.
     * @return The IP address separated into bytes
     */
    public static byte[] toIPv4AddressBytes(final String ipAddress) {
        final String[] octets = ipAddress.split("\\.");
        if (octets.length != 4) {
            throw new IllegalArgumentException("Specified IPv4 address must"
                    + "contain 4 sets of numerical digits separated by periods");
        }

        final byte[] result = new byte[4];
        for (int i = 0; i < 4; ++i) {
            result[i] = Integer.valueOf(octets[i]).byteValue();
        }
        return result;
    }

    /**
     * Accepts an IPv4 address in the form of an integer and returns the
     * corresponding byte array.
     *
     * @param ipAddress
     *            The IP address as an integer.
     * @return The IP address separated into bytes.
     */
    public static byte[] toIPv4AddressBytes(final int ipAddress) {
        return new byte[] {(byte) (ipAddress >>> 24),
                (byte) (ipAddress >>> 16), (byte) (ipAddress >>> 8),
                (byte) ipAddress};
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 2521;
        int result = super.hashCode();
        result = prime * result + this.checksum;
        result = prime * result + this.destinationAddress;
        result = prime * result + this.diffServ;
        result = prime * result + this.flags;
        result = prime * result + this.fragmentOffset;
        result = prime * result + this.headerLength;
        result = prime * result + this.identification;
        result = prime * result + Arrays.hashCode(this.options);
        result = prime * result + this.protocol;
        result = prime * result + this.sourceAddress;
        result = prime * result + this.totalLength;
        result = prime * result + this.ttl;
        result = prime * result + this.version;
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
        if (!(obj instanceof IPv4)) {
            return false;
        }
        final IPv4 other = (IPv4) obj;
        if (this.checksum != other.checksum) {
            return false;
        }
        if (this.destinationAddress != other.destinationAddress) {
            return false;
        }
        if (this.diffServ != other.diffServ) {
            return false;
        }
        if (this.flags != other.flags) {
            return false;
        }
        if (this.fragmentOffset != other.fragmentOffset) {
            return false;
        }
        if (this.headerLength != other.headerLength) {
            return false;
        }
        if (this.identification != other.identification) {
            return false;
        }
        if (!Arrays.equals(this.options, other.options)) {
            return false;
        }
        if (this.protocol != other.protocol) {
            return false;
        }
        if (this.sourceAddress != other.sourceAddress) {
            return false;
        }
        if (this.totalLength != other.totalLength) {
            return false;
        }
        if (this.ttl != other.ttl) {
            return false;
        }
        if (this.version != other.version) {
            return false;
        }
        return true;
    }
}
