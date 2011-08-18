/*
 * Licensed to Nicolas Lalevee under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.highlight.offsets;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

public class HighlightOffsets implements Streamable {

    private int start;

    private int end;

    HighlightOffsets() {
    }

    public HighlightOffsets(int start, int end) {
        this.start = start;
        this.end = end;
    }

    /**
     * The start offset of the highlight in the field
     * 
     * @return
     */
    public int start() {
        return start;
    }

    /**
     * The start offset of the highlight in the field
     * 
     * @return
     */
    public int getStart() {
        return start;
    }

    /**
     * The end offset of the highlight in the field
     * 
     * @return
     */
    public int end() {
        return end;
    }

    /**
     * The end offset of the highlight in the field
     * 
     * @return
     */
    public int getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "[" + start + "->" + end + "]";
    }

    public static HighlightOffsets readHighlightOffsets(StreamInput in) throws IOException {
        HighlightOffsets pos = new HighlightOffsets();
        pos.readFrom(in);
        return pos;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        start = in.readVInt();
        end = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(start);
        out.writeVInt(end);
    }
}
