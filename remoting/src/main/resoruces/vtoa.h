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

#ifndef VTOA_USER_H_INCLUDE
#define VTOA_USER_H_INCLUDE

#include <linux/types.h>

struct vtoa_vs {
	__u32		vid;	/* VPC ID */
	__be32		vaddr;	/* vip */
	__be16		vport;	/* vport */
};

struct vtoa_get_vs {
	struct vtoa_vs vs;
};

struct vtoa_get_vs4rds {
	/* which connection*/
	__u16 protocol;
	__be32 caddr;           /* client address */
	__be16 cport;
	__be32 daddr;           /* destination address */
	__be16 dport;

	/* the virtual servers */
	struct vtoa_vs entrytable[0];
};

#define VTOA_BASE_CTL		(64+1024+64+64+64+64)	/* base */

#define VTOA_SO_GET_VS		(VTOA_BASE_CTL+1)
#define VTOA_SO_GET_VS4RDS	(VTOA_BASE_CTL+2)

#endif