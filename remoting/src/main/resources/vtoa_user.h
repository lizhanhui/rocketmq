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
