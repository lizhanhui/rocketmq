#include <unistd.h>
#include <sys/types.h>

#include "VpcTunnelUtils.h"
#include "vtoa_user.h"

JNIEXPORT jint JNICALL Java_com_alibaba_rocketmq_remoting_vtoa_VpcTunnelUtils_getvip
  (JNIEnv *e, jclass j, jint fd, jobject toa)
{
	jclass cls;
	struct vtoa_get_vs vs={ 0 };
	int len=sizeof(vs);
	int ret=get_vip(fd, &vs, &len);

	if (ret < 0) {
		return ret;
	}

	cls=(*e)->GetObjectClass(e, toa);
	if (cls == NULL)
		return -100;

	jfieldID vid_fd=(*e)->GetFieldID(e, cls, "vid", "I");
	jfieldID vip_fd=(*e)->GetFieldID(e, cls, "vaddr", "I");
	jfieldID vport_fd=(*e)->GetFieldID(e, cls, "vport", "I");

	if (vid_fd == NULL || vip_fd == NULL || vport_fd == NULL)
		return -101;

	(*e)->SetIntField(e, toa, vid_fd, vs.vs.vid);
	(*e)->SetIntField(e, toa, vip_fd, vs.vs.vaddr);
	(*e)->SetIntField(e, toa, vport_fd, vs.vs.vport);

	return 0;
}
