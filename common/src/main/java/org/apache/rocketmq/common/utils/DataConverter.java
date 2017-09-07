package org.apache.rocketmq.common.utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class DataConverter {
	public static Charset charset=Charset.forName("UTF-8");

	public static byte[] Long2Byte(Long v) {
        ByteBuffer tmp = ByteBuffer.allocate(8);
        tmp.putLong(v);
        return tmp.array();
	}
	public static int setBit(int value, int index, boolean flag) {
		if (flag) {
			return value|=(1L << index); 
		}else {
			return value &= ~(1L << index);
		}
	}
	public static boolean getBit(int value, int index) {
		return ((value & (1L << index)) != 0);
	}
}
