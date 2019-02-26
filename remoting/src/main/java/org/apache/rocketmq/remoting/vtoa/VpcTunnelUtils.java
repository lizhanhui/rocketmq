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
package org.apache.rocketmq.remoting.vtoa;

import java.lang.reflect.Field;
import java.net.Socket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DefaultSocketChannelConfig;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class VpcTunnelUtils {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    public static final String PROPERTY_VTOA_TUNNEL_ID = "VTOA_TUNNEL_ID";

    static {
        System.loadLibrary("getvip");
    }

    public native static int getvip(int fd, Vtoa v);
    private static VpcTunnelUtils instance = new VpcTunnelUtils();

    public static VpcTunnelUtils getInstance() {
        return instance;
    }

    public Vtoa getTunnelID(ChannelHandlerContext ctx) {
        Vtoa vtoa = new Vtoa(-1, -1, -1);
        int result = VpcTunnelUtils.getInstance().getvip(getSocketFd(ctx), vtoa);
        if (result == 0 && vtoa != null) {
            log.debug("Get tunnel_id from vtoa success: resultCode={}, vid={}, vaddr={}, vport={}",result, vtoa.getVid(), vtoa.getVaddr(), vtoa.getVport());
        } else {
            log.debug("Get tunnel_id from vtoa error: resultCode={}, vid={}, vaddr={}, vport={}", result, vtoa.getVid(), vtoa.getVaddr(), vtoa.getVport());
        }

        return vtoa;
    }

    /**
     * Fetch socket fd from Netty ChannelHandlerContext
     *
     * @param ctx
     * @return
     */
    private static int getSocketFd(ChannelHandlerContext ctx) {
        try {
            NioSocketChannel nioChannel = (NioSocketChannel)ctx.channel();
            Field configField = nioChannel.getClass().getDeclaredField("config");
            configField.setAccessible(true);
            Object configValue = configField.get(nioChannel);
            configField.set(nioChannel, configValue);
            DefaultSocketChannelConfig config = (DefaultSocketChannelConfig)configValue;

            Field socketField = config.getClass().getSuperclass().getDeclaredField("javaSocket");
            socketField.setAccessible(true);
            Object socketValue = socketField.get(config);
            socketField.set(config, socketValue);
            Socket socket = (Socket)socketValue;

            /* socket channel */
            java.nio.channels.SocketChannel socketChannel = socket.getChannel();

            /* file descriptor */
            Field fileDescriptorField = socketChannel.getClass().getDeclaredField("fd");
            fileDescriptorField.setAccessible(true);
            Object fileDescriptorValue = fileDescriptorField.get(socketChannel);
            fileDescriptorField.set(socketChannel, fileDescriptorValue);
            java.io.FileDescriptor fileDescriptor = (java.io.FileDescriptor)fileDescriptorValue;

            /* fd */
            Field fdField = fileDescriptor.getClass().getDeclaredField("fd");
            fdField.setAccessible(true);
            Object fdValue = fdField.get(fileDescriptor);
            fdField.set(fileDescriptor, fdValue);

            return ((Integer)fdValue).intValue();

        } catch (NoSuchFieldException e) {
            log.error("Get socket field failed. ", e);
        } catch (IllegalAccessException e) {
            log.error("Get socket field failed. ", e);
        }

        return 0;
    }
}