/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.remoting;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manager of all protocols
 *
 * @author tsui
 * @version $Id: ProtocolManager.java, v 0.1 2018-03-27 15:18 tsui Exp $
 */
public class ProtocolManager {

    // Key：协议吗，Value:协议，当前有V1，V2两个协议，并发容器，线程安全
    private static final ConcurrentMap<ProtocolCode, Protocol> protocols = new ConcurrentHashMap<ProtocolCode, Protocol>();

    // 根据协议码获取协议
    public static Protocol getProtocol(ProtocolCode protocolCode) {
        return protocols.get(protocolCode);
    }

    // 注册协议，协议码和协议的映射
    public static void registerProtocol(Protocol protocol, byte... protocolCodeBytes) {
        registerProtocol(protocol, ProtocolCode.fromBytes(protocolCodeBytes));
    }

    // 注册协议，协议码和协议的映射
    public static void registerProtocol(Protocol protocol, ProtocolCode protocolCode) {
        if (null == protocolCode || null == protocol) {
            throw new RuntimeException("Protocol: " + protocol + " and protocol code:"
                                       + protocolCode + " should not be null!");
        }
        Protocol exists = ProtocolManager.protocols.putIfAbsent(protocolCode, protocol);
        if (exists != null) {
            throw new RuntimeException("Protocol for code: " + protocolCode + " already exists!");
        }
    }

    // 根据协议码注销，协议缓存
    public static Protocol unRegisterProtocol(byte protocolCode) {
        return ProtocolManager.protocols.remove(ProtocolCode.fromBytes(protocolCode));
    }
}