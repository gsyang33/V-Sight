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

import java.util.Arrays;

/**
 *
 * @author David Erickson (daviderickson@cs.stanford.edu)
 */
public class Data extends BasePacket {
    protected byte[] data;

    /**
     *
     */
    public Data() {
    }

    /**
     * @param data
     */
    public Data(final byte[] data) {
        this.data = data;
    }

    /**
     * @return the data
     */
    public byte[] getData() {
        return this.data;
    }

    /**
     * @param data
     *            the data to set
     */
    public Data setData(final byte[] data) {
        this.data = data;
        return this;
    }

    @Override
    public byte[] serialize() {
        return this.data;
    }

    @Override
    public IPacket deserialize(final byte[] data, final int offset,
            final int length) {
        this.data = Arrays.copyOfRange(data, offset, data.length);
        return this;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 1571;
        int result = super.hashCode();
        result = prime * result + Arrays.hashCode(this.data);
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
        if (!(obj instanceof Data)) {
            return false;
        }
        final Data other = (Data) obj;
        if (!Arrays.equals(this.data, other.data)) {
            return false;
        }
        return true;
    }
}
