package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.channel.Channel;

public class PopRequest {
	private RemotingCommand remotingCommand;
	private Channel channel;
	private volatile boolean expired;
	
	public PopRequest(RemotingCommand remotingCommand,Channel channel,long expired) {
		this.channel=channel;
		this.remotingCommand=remotingCommand;
	}
	public Channel getChannel() {
		return channel;
	}
	public RemotingCommand getRemotingCommand() {
		return remotingCommand;
	}
	public void setExpired(boolean expired) {
		this.expired = expired;
	}
	public boolean isExpired() {
		return expired;
	}
}
