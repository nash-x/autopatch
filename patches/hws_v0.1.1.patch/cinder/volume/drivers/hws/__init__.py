__author__ = 'Administrator'
import json
import time

from cinder.volume import driver
from hwcloud.database_manager import DatabaseManager
from hwcloud.hws_service.client import HWSClient
from cinder.openstack.common import log as logging
from cinder.volume.drivers.hws import sshutils as sshclient

from oslo.config import cfg

from keystoneclient.v2_0 import client as kc
from cinder.openstack.common import fileutils
from cinder.openstack.common import excutils
from cinder.image import image_utils
import traceback
import string
import os

hws_opts = [cfg.StrOpt('project_id', help='project_id'),
            cfg.StrOpt('flavor_id', help='flavor id'),
            cfg.StrOpt('vpc_id', help='vpc_id'),
            cfg.StrOpt('subnet_id', help='subnet_id'),
            cfg.StrOpt('image_id', help='image_id'),
            cfg.StrOpt('gong_yao', help='gong yao'),
            cfg.StrOpt('si_yao', help='si yao'),
            cfg.StrOpt('service_region', help='region where resource to create in'),
            cfg.StrOpt('resource_region', help='region where resource to create in'),
            cfg.StrOpt('service_protocol', help='protocol', default='https'),
            cfg.StrOpt('service_port', help='port', default='443'),
            cfg.StrOpt('volume_type', help='default volume_typ', default='SATA')]

CONF = cfg.CONF
hws_group = 'hws'
CONF.register_opts(hws_opts, hws_group)

remote_vgw_keystone_opts = [
    cfg.StrOpt('tenant_name',
               default='admin',
               help='tenant name for connecting to keystone in admin context'),
    cfg.StrOpt('user_name',
               default='cloud_admin',
               help='username for connecting to cinder in admin context'),
    cfg.StrOpt('keystone_auth_url',
               default='https://identity.cascading.hybrid.huawei.com:443/identity-admin/v2.0',
               help='value of keystone url'),
]
remote_vgw_keystone_group = 'keystone_authtoken'
CONF.register_opts(remote_vgw_keystone_opts, remote_vgw_keystone_group)


hws_vgw_opts = [
    cfg.StrOpt('user_name',
               default='root',
               help='user name for local az hws v2v gateway host'),
    cfg.StrOpt('password',
               default='Huawei@CLOUD8!',
               help='password for local az hws v2v gateway host'),
    cfg.StrOpt('host_ip',
               default='172.21.0.23',
               help='ip for local az hws v2v gateway host'),
    cfg.StrOpt('ssh_retry_times',
               default='3',
               help='ssh retry times'),
    cfg.StrOpt('hws_instance_id',
               # default='72dca101-e822-4923-a3a1-ffac838ff5d5',
               default='a83325ee-4917-4896-9eac-227f5934115a',
               help='hws vgw instance id'),
    cfg.StrOpt('hws_vgw_ip',
               # default='117.78.35.163',
               default='117.78.36.181',
               help='hws vgw instance id'),
]

hws_vgw_group = 'hws_vgw'
CONF.register_opts(hws_vgw_opts, hws_vgw_group)


LOG = logging.getLogger(__name__)

SATA = 'SATA'
SSD = 'SSD'
SAS = 'SAS'
SUPPORT_VOLUME_TYPE = [SATA, SSD, SAS]

HWS_SERVER_STATUS = {
    'active': 'ACTIVE',
    'shutoff': 'SHUTOFF'
}

HWS_REAL_DEVNAME = {
    '/dev/sda': '/dev/xvda',
    '/dev/sdb': '/dev/xvde',
    '/dev/sdc': '/dev/xvdf',
    '/dev/sdd': '/dev/xvdg',
    '/dev/sde': '/dev/xvdh',
    '/dev/sdf': '/dev/xvdi',
    '/dev/sdg': '/dev/xvdj',
    '/dev/sdh': '/dev/xvdk',
    '/dev/sdi': '/dev/xvdl',
    '/dev/sdj': '/dev/xvdm',
    '/dev/sdk': '/dev/xvdn'
}

class HWSDriver(driver.VolumeDriver):
    VERSION = "1.0"

    def __init__(self, *args, **kwargs):
        super(HWSDriver, self).__init__( *args, **kwargs)
        gong_yao = CONF.hws.gong_yao
        si_yao = CONF.hws.si_yao
        region = CONF.hws.service_region
        protocol = CONF.hws.service_protocol
        port = CONF.hws.service_port
        self.hws_client = HWSClient(gong_yao, si_yao, region, protocol, port)
        self.db_manager = DatabaseManager()
        self.project_id = CONF.hws.project_id
        self.availability_zone = CONF.hws.resource_region
        self.volume_type_default = CONF.hws.volume_type

        self.hws_vgw_user = CONF.hws_vgw.user_name
        self.hws_vgw_password = CONF.hws_vgw.password
        self.hws_wgw_ip = CONF.hws_vgw.host_ip
        self.hws_vgw_ip = CONF.hws_vgw.hws_vgw_ip

    def create_volume(self, volume):
        """Create a volume.
        """
        LOG.info('VOLUME: %s' % dir(volume))
        LOG.info('IMAGE ID: %s' % volume.get('image_id'))
        if not volume.get('image_id'):
            volume_name = volume.display_name
            project_id = self.project_id
            size = volume.size
            volume_type = self.volume_type_default

            job_info = self.hws_client.evs.create_volume(project_id, self.availability_zone,
                                                         size, volume_type, name=volume_name)
            self._deal_with_job(job_info, project_id, self._add_volume_mapping_to_db, None, volume)
        else:
            return {'provider_location': 'HWS CLOUD'}

    def _get_instance_volume_list(self, instance_id):
        """

        :param project_id: string, hws project id
        :param volume_id: string, hws volume id
        :return volume_list_rsp:
        """
        volume_list_rsp = self.hws_client.ecs.get_volume_list(self.project_id, instance_id)
        if volume_list_rsp['status'] != 200:
            error_info = 'hws_v2v: get hws v2v gateway host volume list error, Exception: %s' \
                         % json.dumps(volume_list_rsp)
            LOG.error(error_info)
            raise Exception(error_info)
        return volume_list_rsp

    def _get_volume_detail(self, volume_id):
        """

        :param project_id: string, hws project id
        :param volume_id: string, hws volume id
        :return volume_detail_rsp:
        """
        volume_detail_rsp = self.hws_client.evs.get_volume_detail(self.project_id, volume_id)
        if volume_detail_rsp['status'] != 200:
            error_info = 'hws_v2v: get hws volume detail error, Exception: %s' \
                         % json.dumps(volume_detail_rsp)
            LOG.error(error_info)
            raise Exception(error_info)
        return volume_detail_rsp

    def _attach_volume(self, instance_id, volume_id, device_name):
        """

        :param project: string, hws project id
        :param instance_id: string, hws server id
        :param volume_id: string, hws volume id
        :param device_name: device name, e.g. '/dev/sdb'
        :param cascading_volume_id:  string, cascading volume id
        :return:
        """
        job_attach_volume = self.hws_client.ecs.attach_volume(self.project_id,
                                                              instance_id,
                                                              volume_id,
                                                              device_name)
        self._deal_with_job(job_attach_volume, self.project_id)

    def _deal_java_error(self, java_response):
        """
        {
          'status': 'error',
          'body': {
            'message': '<MESSAGE>',
            'exception': '<EXCEPTION>'
          }
        }
        :param java_response: dict
        :return:
        """
        if 'error' == java_response['status']:
            error_message = java_response['body']['message']
            exception = java_response['body']['exception']
            LOG.error('Java error message: %s, exception: %s' % (error_message, exception))
            raise exception.NovaException(exception)
        if 200 == java_response['status']:
            return
        elif 202 == java_response['status']:
            return
        else:
            error_info = json.dumps(java_response)
            LOG.error(error_info)
            raise Exception(error_info)

    def _power_on(self, instance_id):
        start_result = self.hws_client.ecs.start_server(self.project_id, instance_id)
        self._deal_java_error(start_result)

    def _power_off(self,  instance_id):
        stop_result = self.hws_client.ecs.stop_server(self.project_id, instance_id)
        self._deal_java_error(stop_result)

    def _get_server_status(self, instance_id):
        try:
            server = self.hws_client.ecs.get_detail(self.project_id, instance_id)
            if server and server['status'] == 200:
                status = server['body']['server']['status']
        except Exception:
            msg = traceback.format_exc()
            raise Exception(msg)
        return status

    def _stop_server(self,  instance_id):
        status = self._get_server_status(instance_id)
        if HWS_SERVER_STATUS['active'] == status:
            self._power_off(instance_id)
            time.sleep(20)
            retry_times = 10
            # query server status until server status is SHUTOFF
            while retry_times > 0:
                time.sleep(5)
                status = self._get_server_status(instance_id)
                LOG.error('status: %s' % status)
                if HWS_SERVER_STATUS['shutoff'] == status:
                    break
                retry_times -= 1
        if HWS_SERVER_STATUS['shutoff'] != status:
            msg = "hws_v2v: stop server failed, hws_instance_id: %s, status: %s " %\
                  (instance_id, status)
            raise Exception(msg)

    def _detach_volume(self, instance_id, volume_id):
        """
        Detach the disk attached to the instance.

        :param connection_info:
        {
            u'driver_volume_type': u'vcloud_volume',
            u'serial': u'824d397e-4138-48e4-b00b-064cf9ef4ed8',
            u'data': {
                u'backend': u'vcloud',
                u'qos_specs': None,
                u'access_mode': u'rw',
                u'display_name': u'volume_02',
                u'volume_id': u'824d397e-4138-48e4-b00b-064cf9ef4ed8'
            }
        }
        :param instance:
        :param mountpoint: string, e.g. '/dev/sdb'
        :param encryption:
        :return:
        """
        job_detach_volume = self.hws_client.ecs.detach_volume(self.project_id,
                                                              instance_id,
                                                              volume_id)
        self._deal_with_job(job_detach_volume, self.project_id)

    def _get_instance_next_devname(self, instance_id):
        volume_list_rsp = self._get_instance_volume_list(instance_id)
        volume_list = volume_list_rsp['body']['volumeAttachments']
        used_device_letter = set()
        all_letters = set(string.ascii_lowercase)
        for volume in volume_list:
            used_device_letter.add(volume.get('device')[-1])
        unused_device_letter = list(all_letters - used_device_letter)
        LOG.error(used_device_letter)
        LOG.error(all_letters)
        next_dev_name = volume.get('device')[:-1] + unused_device_letter[0]
        return next_dev_name

    def _get_management_url(self, kc, image_name, **kwargs):
        endpoint_info = kc.service_catalog.get_endpoints(**kwargs)
        endpoint_list = endpoint_info.get(kwargs.get('service_type'), None)
        region_name = image_name.split('_')[-1]
        if endpoint_list:
            for endpoint in endpoint_list:
                if region_name == endpoint.get('region'):
                    return endpoint.get('publicURL')

    def _copy_volume_to_file(self, image_meta, dev_name):
        image_id = image_meta.get('id')
        dest_file_path = os.path.join('/tmp', image_id)
        real_devname = HWS_REAL_DEVNAME[dev_name]
        try:
            ssh_client = sshclient.SSH(user=self.hws_vgw_user,
                                       host=self.hws_vgw_ip,
                                       password=self.hws_vgw_password)
            # convert volume to image
            cmd = 'qemu-img convert -c -O qcow2 %s %s' % \
                  (real_devname, dest_file_path)
            LOG.error('begin time of %s is %s' %
                      (cmd, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()
                                          )))
            ssh_client.run(cmd)
            LOG.debug("Finished running cmd : %s" % cmd)
            LOG.error('end time of %s is %s' %
                      (cmd, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error('Failed to copy volume to image by vgw.',
                          traceback.format_exc())
        finally:
            if ssh_client:
                # delete the temp file which is used for convert volume to image
                ssh_client.close()

    @sshclient.RetryDecorator(max_retry_count=CONF.hws_vgw.ssh_retry_times,
                    exceptions=(sshclient.SSHError, sshclient.SSHTimeout))
    def _copy_file_to_remote_vgw(self, image_meta):
        image_id = image_meta.get('id')
        image_name = image_meta.get('name')
        dest_file_path = os.path.join('/tmp', image_id)
        kwargs = {
            'auth_url': CONF.keystone_authtoken.keystone_auth_url,
            'tenant_name': CONF.keystone_authtoken.tenant_name,
            'user_name': CONF.keystone_authtoken.user_name,
            'password': CONF.keystone_authtoken.password,
            'insecure': True
        }
        keystone_client = kc.Client(**kwargs)
        # get remote v2v gateway
        vgw_url = self._get_management_url(keystone_client, image_name, service_type='v2v')
        try:
            ssh_client = sshclient.SSH(user=self.hws_vgw_user,
                                       host=self.hws_vgw_ip,
                                       password=self.hws_vgw_password)
            LOG.debug('The remote vgw url is %(vgw_url)s',
                      {'vgw_url': vgw_url})
            # eg: curl -X POST --http1.0 -T
            # /tmp/467bd6e1-5a6e-4daa-b8bc-356b718834f2
            # http://172.27.12.245:8090/467bd6e1-5a6e-4daa-b8bc-356b718834f2
            cmd = 'curl -X POST --http1.0 -T %s ' % dest_file_path
            cmd += vgw_url
            if cmd.endswith('/'):
                cmd += image_id
            else:
                cmd += '/' + image_id
            LOG.error('begin time of %s is %s' %
                      (cmd, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()
                                          )))
            ssh_client.run(cmd)
            LOG.error('end time of %s is %s' %
                      (cmd, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()
                                          )))

            LOG.debug("Finished running cmd : %s" % cmd)
            ssh_client.run('rm -f %s' % dest_file_path)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error('Failed to copy volume to image by vgw.',
                          traceback.format_exc())
        finally:
            if ssh_client:
                ssh_client.close()

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        container_format = image_meta.get('container_format')
        #if container_format == 'vgw_url':
        if container_format == 'bare':
            try:
                # 1.get the hws volume id
                cascaded_volume_id = volume['id']
                hws_volume_id = self.db_manager.get_cascaded_volume_id(cascaded_volume_id)
                if not hws_volume_id:
                    msg = 'get hws volume id error, cascaded id: %s' % cascaded_volume_id
                    LOG.error(msg)
                    raise Exception('get hws volume id error')
                # 2. get the hws_volume status
                volume_detail_rsp = self._get_volume_detail(hws_volume_id)
                status = volume_detail_rsp['body']['volume']['status']
                # attachments = volume_detail_rsp['body']['volume']['attachments']
                # attach_num = len(attachments)
                # origin_instance_id = None
                # attach_back = False
                # 3. detach volume from origin instance
                # if status == 'in-use':
                #     if attach_num != 1:
                #         msg = 'hws_v2v: get attachments info error, num: %s' % attach_num
                #         LOG.error(msg)
                #         raise Exception(msg)
                #     origin_instance_id = attachments[0]['server_id']
                #     # volume can only be detached when sever stop
                #     self._stop_server(origin_instance_id)
                #     self._detach_volume(origin_instance_id, hws_volume_id)
                #     attach_back = True
                #     volume_detail_rsp = self._get_volume_detail(hws_volume_id)
                #     status = volume_detail_rsp['body']['status']

                # 4. attach volume to hws v2v gateway host
                if status != 'available':
                    msg = 'attach volume to local v2v gateway host error, status : %s, cascaded_volume_id: %s, ' \
                          'hws_volume_id %s' % (status, cascaded_volume_id, hws_volume_id)
                    LOG.error(msg)
                    raise Exception('attach volume to local v2v gateway failed')
                hws_vgw_instance_id = CONF.hws_vgw.hws_instance_id
                # if not hws_vgw_instance_id:
                #     LOG.error(
                #         'hws_v2v: get cascaded v2v gateway instance id error: %s' % CONF.hws_vgw.cascaded_instance_id)
                #     raise Exception('hws_v2v: get cascaded v2v gateway instance error.')
                dev_name = self._get_instance_next_devname(hws_vgw_instance_id)
                self._attach_volume(hws_vgw_instance_id, hws_volume_id, dev_name)
                # 5. copy volume to file
                self._copy_volume_to_file(image_meta, dev_name)

                # 6. copy file to remote v2v gateway
                # self._copy_file_to_remote_vgw(image_meta)
                # 7. create a empty file to glance
                with image_utils.temporary_file() as tmp:
                    image_utils.upload_volume(context,
                                              image_service,
                                              image_meta,
                                              tmp)
                fileutils.delete_if_exists(tmp)
                # 8. detach volume from hws v2v gateway
                self._stop_server(hws_vgw_instance_id)
                self._detach_volume(hws_vgw_instance_id, hws_volume_id)
                self._power_on(hws_vgw_instance_id)
            finally:
                attach_back = True
                # if attach_back is True:
                #     origin_dev_name = attachments[0]['device']
                #     self._attach_volume(origin_instance_id, hws_volume_id, origin_dev_name)
                #     self._power_on(origin_instance_id)

    @sshclient.RetryDecorator(max_retry_count=CONF.hws_vgw.ssh_retry_times,
                                  exceptions=(sshclient.SSHError, sshclient.SSHTimeout))
    def _copy_file_to_volume(self, image_id, dev_name):
        try:
            real_devname = HWS_REAL_DEVNAME[dev_name]
            dest_file_path = os.path.join('/tmp', image_id)
            ssh_client = sshclient.SSH(user=self.hws_vgw_user,
                                       host=self.hws_vgw_ip,
                                       password=self.hws_vgw_password)
            # copy data to volume
            cmd = 'qemu-img convert %s %s' % \
                  (dest_file_path, real_devname)
            ssh_client.run(cmd)
            LOG.debug("Finished running cmd : %s" % cmd)

            # cmd = 'rm -rf %s' % dest_file_path
            # ssh_client.run(cmd)

        except Exception as e:
            LOG.error('Failed to copy data to volume from vgw. '
                      'traceback: %s', traceback.format_exc())
            raise e
        finally:
            if ssh_client:
                ssh_client.close()

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        image_meta = image_service.show(context, image_id)
        container_format = image_meta.get('container_format')
        # if container_format == 'vgw_url':
        if container_format == 'bare':
            # 1.get the hws_volume_id
            cascaded_volume_id = volume['id']
            self.create_volume(volume)
            hws_volume_id = self.db_manager.get_cascaded_volume_id(cascaded_volume_id)
            if not cascaded_volume_id:
                LOG.error('get cascaded volume id error: %s' % cascaded_volume_id)
                raise Exception('get cascaded volume id error.')
            # 2. get the hws_volume status
            time.sleep(30)
            retry_times = 10
            while retry_times > 0:
                volume_detail_rsp = self._get_volume_detail(hws_volume_id)
                status = volume_detail_rsp['body']['volume']['status']
                if status == 'available':
                    break
                else:
                    time.sleep(5)
                    retry_times -= 1
            if status != 'available':
                LOG.error('create hws volume failed, status: %s, cascaded_volume_id: %s, hws_volume_id: %s'
                          % (status, cascaded_volume_id, hws_volume_id))
                raise Exception('create hws volume failed.')
            # 2. attach volume to hws v2v gateway host
            hws_vgw_instance_id = CONF.hws_vgw.hws_instance_id
            # if not hws_vgw_instance_id:
            #     LOG.error('hws_v2v: get cascaded v2v gateway instance id error.' % CONF.hws_vgw.cascaded_instance_id)
            #     raise Exception('get cascaded v2v gateway instance id error.')
            dev_name = self._get_instance_next_devname(hws_vgw_instance_id)
            self._attach_volume(hws_vgw_instance_id, hws_volume_id, dev_name)
            # 3. copy image's file to volume
            self._copy_file_to_volume(image_id, dev_name)

            # 4. detach volume from hws v2v gateway
            self._stop_server(hws_vgw_instance_id)
            self._detach_volume(hws_vgw_instance_id, hws_volume_id)
            self._power_on(hws_vgw_instance_id)

        # Not to create volume when call cinder create volume API
        # Only when attache or dettach, or create server by volume, then create volume.
        elif not image_id:
            volume_name = volume.display_name
            project_id = self.project_id
            size = volume.size
            volume_type = self.volume_type_default
            image_hws_id = self._get_cascaded_image_id(image_id)

            job_info = self.hws_client.evs.create_volume(project_id, self.availability_zone,
                                                         size, volume_type, name=volume_name, imageRef=image_hws_id)
            self._deal_with_job(job_info, project_id, self._add_volume_mapping_to_db, None, volume)

    def _get_volume_type(self, volume_type):
        if volume_type not in SUPPORT_VOLUME_TYPE:
            LOG.info('VOLUME TYPE: %s is not support in HWS Clouds, support type is: [%s]. Use SATA as default' %
                     (volume_type, SUPPORT_VOLUME_TYPE))
            volume_type = SATA

        return volume_type

    def _get_cascaded_image_id(self, cascading_image_id):
        cascaded_image_id = self.db_manager.get_cascaded_image_id(cascading_image_id)
        if not cascaded_image_id:
            LOG.error('No image mapping in HWS Cloud.')
            raise Exception('No image mapping in HWS Cloud.')

        return cascaded_image_id

    def _add_volume_mapping_to_db(self, job_detail_of_create_volume, volume):
        """

        :param job_detail_of_create_volume:
        :return:
        """
        hws_volume_id = job_detail_of_create_volume['body']['entities']['volume_id']
        volume_id = volume.id
        self.db_manager.add_volume_mapping(volume_id, hws_volume_id)
        LOG.info('Success to add volume mapping: {%s: %s}' % (volume_id, hws_volume_id))

    def _deal_with_job(self, job_info, project_id,
                       function_deal_with_success=None,
                       function_deal_with_fail=None,
                       object=None):
        if job_info['status'] == 200:
            job_id = job_info['body']['job_id']
            while True:
                time.sleep(5)
                job_detail_info = self.hws_client.evs.get_job_detail(project_id, job_id)
                if job_detail_info:
                    if job_detail_info['status'] == 200:
                        job_status = job_detail_info['body']['status']
                        if job_status == 'RUNNING':
                            LOG.debug('job<%s> is still RUNNING.' % job_id)
                            continue
                        elif job_status == 'FAIL':
                            if function_deal_with_fail:
                                function_deal_with_fail(job_detail_info, object)
                            error_info = 'job<%s> FAIL, ERROR INFO: %s' % (job_id, json.dumps(job_detail_info))
                            raise Exception(error_info)
                        elif job_status == 'SUCCESS':
                            if function_deal_with_success:
                                function_deal_with_success(job_detail_info, object)
                            success_info = 'job<%s> SUCCESS.' % job_id
                            LOG.info(success_info)
                            break
                    elif job_detail_info['status'] == 'error':
                        error_message = job_detail_info['body']['message']
                        exception = job_detail_info['body']['exception']
                        LOG.error('Java error message: %s, exception: %s' % (error_message, exception))
                        continue
                    else:
                        info = json.dumps(job_detail_info)
                        LOG.info('Job info get has some issue: %s, will retry to get again.' % info )
                        continue
                else:
                    retry_info = 'job detail info is empty, will retry to get. JOB DETAIL: %s' % job_detail_info
                    LOG.info(retry_info)
                    continue
        else:
            error_info = json.dumps(job_info)
            LOG.error('Job init FAIL, error info: %s' % error_info)
            raise Exception(error_info)

    def _deal_with_create_volume_fail(self, job_detail_info, volume):
        """
        deal with create volume fail.
        If hws volume is created, but fail, then save id mapping in db. then raise exception.
        if hws volume id is not created, raise exception directly.
        {
            "body": {
                "status": "FAIL",
                "entities": {
                    "volume_id": "1be7a768-59b6-4ef6-b4c0-a4f8039fa626"
                },
                "job_id": "8aace0c751b0a3bd01523529e4f70d35",
                "job_type": "createVolume",
                "begin_time": "2016-01-12T09:28:04.086Z",
                "end_time": "2016-01-12T09:28:32.252Z",
                "error_code": "EVS.2024",
                "fail_reason": "EbsCreateVolumeTask-fail:volume is error!"
            },
            "status": 200
        }
        :param job_detail_info:
        :param volume:
        :return:
        """
        job_id = job_detail_info.get('body').get('job_id')
        error_info = 'job<%s> FAIL, ERROR INFO: %s' % (job_id, json.dumps(job_detail_info))
        if job_detail_info.get('body').get('entities'):
            hws_volume_id = job_detail_info.get('body').get('entities').get('volume_id')
            if hws_volume_id:
                LOG.info('HWS volume is created, id is: %s' % hws_volume_id)
                volume_id = volume.id
                self.db_manager.add_volume_mapping(volume_id, hws_volume_id)
                LOG.debug('Success to add volume mapping: {%s: %s}' % (volume_id, hws_volume_id))
                raise Exception(error_info)

        raise Exception(error_info)

    def delete_volume(self, volume):
        cascading_volume_id = volume.id
        project_id = self.project_id
        cascaded_volume_id = self.db_manager.get_cascaded_volume_id(cascading_volume_id)
        LOG.info('VOLUME_ID: %s' % cascaded_volume_id)

        if cascaded_volume_id:
            volume_get = self.hws_client.evs.get_volume_detail(project_id, cascaded_volume_id)
            if volume_get['status'] == 200:
                job_info = self.hws_client.evs.delete_volume(project_id, cascaded_volume_id)
                self._deal_with_job(job_info,project_id, self._delete_volume_mapping, None, volume)
            elif volume_get['status'] == 404 and volume_get.get('body').get('itemNotFound'):
                LOG.info('cascaded volume is not exist, so directly return delete success')
                return
            else:
                error_info = 'Delete volume fail, Exception: %s' % json.dumps(volume_get)
                LOG.error(error_info)
                raise Exception(error_info)
        else:
            LOG.info('cascaded volume is not exist, so directly return delete success')
            return

    def _delete_volume_mapping(self, job_detail_info, volume):
        cascading_volume_id = volume.id
        self.db_manager.delete_volume_mapping(cascading_volume_id)
        LOG.info('Delete volume mapping for cascading volume id: %s' % cascading_volume_id)

    def get_volume_stats(self, refresh=False):
        """Get volume stats."""
        # pdb.set_trace()
        if not self._stats:
            backend_name = self.configuration.safe_get('volume_backend_name')
            LOG.debug('*******backend_name is %s' %backend_name)
            if not backend_name:
                backend_name = 'HC_HWS'
            data = {'volume_backend_name': backend_name,
                    'vendor_name': 'Huawei',
                    'driver_version': self.VERSION,
                    'storage_protocol': 'LSI Logic SCSI',
                    'reserved_percentage': 0,
                    'total_capacity_gb': 1000,
                    'free_capacity_gb': 1000}
            self._stats = data
        return self._stats

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info."""
        LOG.debug('vCloud Driver: initialize_connection')

        driver_volume_type = 'hwclouds_volume'
        data = {}
        data['backend'] = 'hwclouds'
        data['volume_id'] = volume['id']
        data['display_name'] = volume['display_name']

        return {'driver_volume_type': driver_volume_type,
                 'data': data}

    def check_for_setup_error(self):
        """Check configuration file."""
        pass

    def create_cloned_volume(self, volume, src_vref):
        """Create a clone of the specified volume."""
        pass

    def create_export(self, context, volume):
        """Export the volume."""
        pass

    def create_snapshot(self, snapshot):
        pass

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a volume from a snapshot."""
        pass

    def delete_snapshot(self, snapshot):
        """Delete a snapshot."""
        pass

    def do_setup(self, context):
        """Instantiate common class and log in storage system."""
        pass

    def ensure_export(self, context, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def extend_volume(self, volume, new_size):
        """Extend a volume."""
        pass

    def remove_export(self, context, volume):
        """Remove an export for a volume."""
        pass

    def terminate_connection(self, volume, connector, **kwargs):
        """Disallow connection from connector"""
        LOG.debug('vCloud Driver: terminate_connection')
        pass

    def validate_connector(self, connector):
        """Fail if connector doesn't contain all the data needed by driver."""
        LOG.debug('vCloud Driver: validate_connector')
        pass



