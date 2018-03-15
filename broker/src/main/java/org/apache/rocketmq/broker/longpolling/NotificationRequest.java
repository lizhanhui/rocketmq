package org.apache.rocketmq.broker.longpolling;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.channel.Channel;

public class NotificationRequest {
	private RemotingCommand remotingCommand;
	private Channel channel;
	private  long expired;
	private AtomicBoolean complete=new AtomicBoolean(false);
	public NotificationRequest(RemotingCommand remotingCommand,Channel channel,long expired) {
		this.channel=channel;
		this.remotingCommand=remotingCommand;
		this.expired=expired;
	}
	public Channel getChannel() {
		return channel;
	}
	public RemotingCommand getRemotingCommand() {
		return remotingCommand;
	}
	public boolean isTimeout() {
		return System.currentTimeMillis() > (expired - 3000);
	}
	public boolean complete(){
		return complete.compareAndSet(false, true);
	}
	@Override
	public String toString() {
		return remotingCommand.toString();
	}
}
