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
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "vtoa.h"

int get_vip4rds(int sockfd, struct vtoa_get_vs4rds *vs, int *len)
{	
	struct sockaddr_in saddr, daddr;
	int ret, saddrlen, daddrlen;

	if (*len != sizeof(struct vtoa_get_vs4rds) + sizeof(struct vtoa_vs))
		return -EINVAL;
	
	saddrlen = sizeof(saddr);
	if (ret = getpeername(sockfd, (struct sockaddr *)&saddr, &saddrlen) < 0)
		return ret;

	daddrlen = sizeof(daddr);
	if (ret = getsockname(sockfd, (struct sockaddr *)&daddr, &daddrlen) < 0)
		return ret;

	vs->protocol = IPPROTO_TCP;
	vs->caddr = saddr.sin_addr.s_addr;
	vs->cport = saddr.sin_port;
	vs->daddr = daddr.sin_addr.s_addr;
	vs->dport = daddr.sin_port;

	return getsockopt(sockfd, IPPROTO_IP, VTOA_SO_GET_VS4RDS, vs, len);
}

int get_vtoa(int sockfd, struct vtoa_get_vs *vs, int *len)
{	
	if (*len != sizeof(struct vtoa_get_vs))
		return -EINVAL;
	
	return getsockopt(sockfd, IPPROTO_IP, VTOA_SO_GET_VS, vs, len);
}