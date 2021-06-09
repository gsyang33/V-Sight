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
import java.util.ArrayList;
import java.util.List;

/**
 * @author David Erickson (daviderickson@cs.stanford.edu)
 *
 */
public class LLDP extends BasePacket {
    protected LLDPTLV chassisId;
    protected LLDPTLV portId;
    protected LLDPTLV ttl;
    protected List<LLDPTLV> optionalTLVList;
    protected short ethType;

    public LLDP() {
        this.optionalTLVList = new ArrayList<LLDPTLV>();
        this.ethType = Ethernet.TYPE_LLDP;
    }

    /**
     * @return the chassisId
     */
    public LLDPTLV getChassisId() {
        return this.chassisId;
    }

    /**
     * @param chassisId
     *            the chassisId to set
     */
    public LLDP setChassisId(final LLDPTLV chassisId) {
        this.chassisId = chassisId;
        return this;
    }

    /**
     * @return the portId
     */
    public LLDPTLV getPortId() {
        return this.portId;
    }

    /**
     * @param portId
     *            the portId to set
     */
    public LLDP setPortId(final LLDPTLV portId) {
        this.portId = portId;
        return this;
    }

    /**
     * @return the ttl
     */
    public LLDPTLV getTtl() {
        return this.ttl;
    }

    /**
     * @param ttl
     *            the ttl to set
     */
    public LLDP setTtl(final LLDPTLV ttl) {
        this.ttl = ttl;
        return this;
    }

    /**
     * @return the optionalTLVList
     */
    public List<LLDPTLV> getOptionalTLVList() {
        return this.optionalTLVList;
    }

    /**
     * @param optionalTLVList
     *            the optionalTLVList to set
     */
    public LLDP setOptionalTLVList(final List<LLDPTLV> optionalTLVList) {
        this.optionalTLVList = optionalTLVList;
        return this;
    }

    @Override
    public byte[] serialize() {
        int length = 2 + this.chassisId.getLength() + 2
                + this.portId.getLength() + 2 + this.ttl.getLength() + 2;
        for (final LLDPTLV tlv : this.optionalTLVList) {
            length += 2 + tlv.getLength();
        }

        final byte[] data = new byte[length];
        final ByteBuffer bb = ByteBuffer.wrap(data);
        bb.put(this.chassisId.serialize());
        bb.put(this.portId.serialize());
        bb.put(this.ttl.serialize());
        for (final LLDPTLV tlv : this.optionalTLVList) {
            bb.put(tlv.serialize());
        }
        bb.putShort((short) 0); // End of LLDPDU

        /*
         * if (this.parent != null && this.parent instanceof Ethernet) {
         * ((Ethernet) this.parent).setEtherType(this.ethType); }
         */

        return data;
    }

    @Override
    public IPacket deserialize(final byte[] data, final int offset,
            final int length) {
        final ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
        LLDPTLV tlv;
        do {
            tlv = new LLDPTLV().deserialize(bb);

            // if there was a failure to deserialize stop processing TLVs
            if (tlv == null) {
                break;
            }
            switch (tlv.getType()) {
            case 0x0:
                // can throw this one away, its just an end delimiter
                break;
            case 0x1:
                this.chassisId = tlv;
                break;
            case 0x2:
                this.portId = tlv;
                break;
            case 0x3:
                this.ttl = tlv;
                break;
            default:
                this.optionalTLVList.add(tlv);
                break;
            }
        } while (tlv.getType() != 0 && bb.hasRemaining());
        return this;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 883;
        int result = super.hashCode();
        result = prime * result
                + (this.chassisId == null ? 0 : this.chassisId.hashCode());
        result = prime * result + this.optionalTLVList.hashCode();
        result = prime * result
                + (this.portId == null ? 0 : this.portId.hashCode());
        result = prime * result + (this.ttl == null ? 0 : this.ttl.hashCode());
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
        if (!(obj instanceof LLDP)) {
            return false;
        }
        final LLDP other = (LLDP) obj;
        if (this.chassisId == null) {
            if (other.chassisId != null) {
                return false;
            }
        } else if (!this.chassisId.equals(other.chassisId)) {
            return false;
        }
        if (!this.optionalTLVList.equals(other.optionalTLVList)) {
            return false;
        }
        if (this.portId == null) {
            if (other.portId != null) {
                return false;
            }
        } else if (!this.portId.equals(other.portId)) {
            return false;
        }
        if (this.ttl == null) {
            if (other.ttl != null) {
                return false;
            }
        } else if (!this.ttl.equals(other.ttl)) {
            return false;
        }
        return true;
    }
}
