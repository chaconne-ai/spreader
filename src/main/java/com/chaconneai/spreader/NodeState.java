/*
 * Copyright 2026 ChaconneAI
 *
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
package com.chaconneai.spreader;

/**
 * State of a node. Which state wins when two disagree is decided by
 * {@link #priority()}: at equal incarnation, the higher priority overrides the lower.
 * That is the standard SWIM rule.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum NodeState {

    /** Alive and serving normally. */
    ALIVE((byte) 1, 0),

    /** Suspect: a probe timed out, but death is not confirmed. Still a member. */
    SUSPECT((byte) 2, 1),

    /** Confirmed dead -- not a graceful exit, but a crash or a network partition. */
    DEAD((byte) 3, 2),

    /** Left gracefully: {@code stop()} was called and a LEAVE was broadcast. */
    LEFT((byte) 4, 3);

    private final byte code;
    private final int priority;

    NodeState(byte code, int priority) {
        this.code = code;
        this.priority = priority;
    }

    public byte code() {
        return code;
    }

    /** Override priority: the higher the number, the more certain the state, and the more it wins. */
    public int priority() {
        return priority;
    }

    /** Whether this is still an effective member -- gossiping, and eligible to lead. */
    public boolean isMember() {
        return this == ALIVE || this == SUSPECT;
    }

    /** Whether this is terminal: gone, and kept only as a tombstone long enough to propagate. */
    public boolean isTerminal() {
        return this == DEAD || this == LEFT;
    }

    public static NodeState fromCode(byte code) {
        for (NodeState s : values()) {
            if (s.code == code) {
                return s;
            }
        }
        throw new IllegalArgumentException("unknown node state code: " + code);
    }
}
