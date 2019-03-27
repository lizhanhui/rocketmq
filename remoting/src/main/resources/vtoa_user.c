#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "vtoa_user.h"

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

int get_vip(int sockfd, struct vtoa_get_vs *vs, int *len)
{
	if (*len != sizeof(struct vtoa_get_vs))
		return -EINVAL;

	return getsockopt(sockfd, IPPROTO_IP, VTOA_SO_GET_VS, vs, len);
}
