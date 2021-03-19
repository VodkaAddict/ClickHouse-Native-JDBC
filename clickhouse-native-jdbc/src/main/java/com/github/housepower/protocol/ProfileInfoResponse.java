/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.housepower.protocol;

import com.github.housepower.io.CompositeSource;

import java.io.IOException;

public class ProfileInfoResponse implements Response {

    public static ProfileInfoResponse readFrom(CompositeSource source) throws IOException {
        long rows = source.readVarInt();
        long blocks = source.readVarInt();
        long bytes = source.readVarInt();
        long appliedLimit = source.readVarInt();
        long rowsBeforeLimit = source.readVarInt();
        boolean calculatedRowsBeforeLimit = source.readBoolean();
        return new ProfileInfoResponse(rows, blocks, bytes, appliedLimit, rowsBeforeLimit, calculatedRowsBeforeLimit);
    }

    private final long rows;
    private final long blocks;
    private final long bytes;
    private final long appliedLimit;
    private final long rowsBeforeLimit;
    private final boolean calculatedRowsBeforeLimit;

    public ProfileInfoResponse(long rows, long blocks, long bytes,
                               long appliedLimit, long rowsBeforeLimit, boolean calculatedRowsBeforeLimit) {
        this.rows = rows;
        this.blocks = blocks;
        this.bytes = bytes;
        this.appliedLimit = appliedLimit;
        this.rowsBeforeLimit = rowsBeforeLimit;
        this.calculatedRowsBeforeLimit = calculatedRowsBeforeLimit;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_PROFILE_INFO;
    }

    public long rows() {
        return rows;
    }

    public long blocks() {
        return blocks;
    }

    public long bytes() {
        return bytes;
    }

    public long appliedLimit() {
        return appliedLimit;
    }

    public long rowsBeforeLimit() {
        return rowsBeforeLimit;
    }

    public boolean calculatedRowsBeforeLimit() {
        return calculatedRowsBeforeLimit;
    }
}
