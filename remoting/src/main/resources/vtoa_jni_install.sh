#!/bin/sh

#Init VpcTunnelUtils.h with javah
cd ./remoting/src/main/java && javah -o VpcTunnelUtils.h -classpath . -jni org.apache.rocketmq.remoting.vtoa.VpcTunnelUtils

#Build and install JNI lib
sudo gcc -I /opt/taobao/java/include/  -I /opt/taobao/java/include/linux/  -c VpcTunnelUtils.c  -fPIC
sudo gcc  -I /usr/lib/jvm/java-openjdk/include  -I /usr/lib/jvm/java-openjdk/include/linux/  -c vtoa_user.c  -fPIC
sudo gcc -shared -fPIC  -o libgetvip.so VpcTunnelUtils.o vtoa_user.o
sudo cp libgetvip.so /usr/lib/
