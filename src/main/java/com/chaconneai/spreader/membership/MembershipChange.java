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
package com.chaconneai.spreader.membership;

import com.chaconneai.spreader.Node;

/**
 * A single change to the membership view.
 *
 * @param type     what kind of change this is
 * @param node     the node as it looks after the change
 * @param previous the node as it looked before; null when it has just joined
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record MembershipChange(Type type, Node node, Node previous) {

    /** Kinds of membership change. */
    public enum Type {
        /** A new member joined. */
        JOINED,
        /** A member left gracefully. */
        LEFT,
        /** A member was declared failed. */
        DEAD,
        /** A member became suspect. */
        SUSPECT,
        /** A member recovered from being suspect. */
        RECOVERED,
        /** A member's metadata changed. */
        UPDATED
    }

    public static MembershipChange joined(Node node) {
        return new MembershipChange(Type.JOINED, node, null);
    }

    public static MembershipChange of(Type type, Node node, Node previous) {
        return new MembershipChange(type, node, previous);
    }
}
