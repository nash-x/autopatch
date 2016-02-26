import socket
import traceback
import json
import random
import string
import time

from nova.virt import driver
from nova.openstack.common import jsonutils
from nova.compute import power_state
from nova import exception
from nova.openstack.common import loopingcall
from nova.openstack.common import log as logging
from nova.volume.cinder import API as cinder_api

from hwcloud.hws_service.client import HWSClient
from hwcloud.database_manager import DatabaseManager
from oslo.config import cfg

LOG = logging.getLogger(__name__)

HWS_DOMAIN_NOSTATE = 0
HWS_DOMAIN_RUNNING = 1
HWS_DOMAIN_BLOCKED = 2
HWS_DOMAIN_PAUSED = 3
HWS_DOMAIN_SHUTDOWN = 4
HWS_DOMAIN_SHUTOFF = 5
HWS_DOMAIN_CRASHED = 6
HWS_DOMAIN_PMSUSPENDED = 7

HWS_POWER_STATE = {
    HWS_DOMAIN_NOSTATE: power_state.NOSTATE,
    HWS_DOMAIN_RUNNING: power_state.RUNNING,
    HWS_DOMAIN_BLOCKED: power_state.RUNNING,
    HWS_DOMAIN_PAUSED: power_state.PAUSED,
    HWS_DOMAIN_SHUTDOWN: power_state.SHUTDOWN,
    HWS_DOMAIN_SHUTOFF: power_state.SHUTDOWN,
    HWS_DOMAIN_CRASHED: power_state.CRASHED,
    HWS_DOMAIN_PMSUSPENDED: power_state.SUSPENDED,
}

SATA = 'SATA'
SSD = 'SSD'
SAS = 'SAS'
SUPPORT_VOLUME_TYPE = [SATA, SSD, SAS]

hws_opts = [
    cfg.StrOpt('project_id',
               help='project_id'),
    cfg.StrOpt('flavor_id',
               help='flavor id'),
    cfg.StrOpt('vpc_id',
               help='vpc_id'),
    cfg.StrOpt('subnet_id',
               help='subnet_id'),
    cfg.StrOpt('image_id',
               help='image_id'),
    cfg.StrOpt('gong_yao',
               help='gong yao'),
    cfg.StrOpt('si_yao',
               help='si yao'),
    cfg.StrOpt('service_region', help='service region'),
    cfg.StrOpt('resource_region', help='resource_region')
    ]

CONF = cfg.CONF
hws_group = 'hws'
CONF.register_opts(hws_opts, hws_group)

class HwsComputeDriver(driver.ComputeDriver):

    def __init__(self, virtapi):
        super(HwsComputeDriver, self).__init__(virtapi)
        # self.nova_client = NovaService()
        gong_yao = CONF.hws.gong_yao
        si_yao = CONF.hws.si_yao
        region = CONF.hws.service_region
        protocol = "https"
        port = "443"
        self.project = CONF.hws.project_id
        self.hws_client = HWSClient(gong_yao, si_yao, region, protocol, port)
        self.cinder_api = cinder_api()

    def _transfer_to_host_server_name(self, instance_uuid):
        """
        Transfer instance uuid to server name with format 'hws-server@<UUID>',
        This server name will be use server name in hws node.

        :param instance_uuid: e.g. '7c615d1c-07d3-4730-bf7a-ef7ad464c8fd'
        :return: e.g. 'hws-server@7c615d1c-07d3-4730-bf7a-ef7ad464c8fd'
        """
        return '@'.join(['hws-server', instance_uuid])

    def _transfer_to_uuid(self, server_name):
        """
        Use to transfer name of server in hws node to local instance uuid.

        :param server_name: e.g. 'hws-server@7c615d1c-07d3-4730-bf7a-ef7ad464c8fd'
        :return: 7c615d1c-07d3-4730-bf7a-ef7ad464c8fd
        """
        return server_name.split('@', 1)[1]

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        """Create a new instance/VM/domain on the virtualization platform.

        Once this successfully completes, the instance should be
        running (power_state.RUNNING).

        If this fails, any partial instance should be completely
        cleaned up, and the virtualization platform should be in the state
        that it was before this call began.

        :param context: security context
        :param instance: nova.objects.instance.Instance
                         This function should use the data there to guide
                         the creation of the new instance.
                         Instance(
                             access_ip_v4=None,
                             access_ip_v6=None,
                             architecture=None,
                             auto_disk_config=False,
                             availability_zone='az31.shenzhen--aws',
                             cell_name=None,
                             cleaned=False,
                             config_drive='',
                             created_at=2015-08-31T02:44:36Z,
                             default_ephemeral_device=None,
                             default_swap_device=None,
                             deleted=False,
                             deleted_at=None,
                             disable_terminate=False,
                             display_description='server@daa5e17c-cb2c-4014-9726-b77109380ca6',
                             display_name='server@daa5e17c-cb2c-4014-9726-b77109380ca6',
                             ephemeral_gb=0,
                             ephemeral_key_uuid=None,
                             fault=<?>,
                             host='42085B38-683D-7455-A6A3-52F35DF929E3',
                             hostname='serverdaa5e17c-cb2c-4014-9726-b77109380ca6',
                             id=49,
                             image_ref='6004b47b-d453-4695-81be-cd127e23f59e',
                             info_cache=InstanceInfoCache,
                             instance_type_id=2,
                             kernel_id='',
                             key_data=None,
                             key_name=None,
                             launch_index=0,
                             launched_at=None,
                             launched_on='42085B38-683D-7455-A6A3-52F35DF929E3',
                             locked=False,
                             locked_by=None,
                             memory_mb=512,
                             metadata={},
                             node='h',
                             numa_topology=None,
                             os_type=None,
                             pci_devices=<?>,
                             power_state=0,
                             progress=0,
                             project_id='52957ad92b2146a0a2e2b3279cdc2c5a',
                             ramdisk_id='',
                             reservation_id='r-d1dkde4x',
                             root_device_name='/dev/sda',
                             root_gb=1,
                             scheduled_at=None,
                             security_groups=SecurityGroupList,
                             shutdown_terminate=False,
                             system_metadata={
                                 image_base_image_ref='6004b47b-d453-4695-81be-cd127e23f59e',
                                 image_container_format='bare',
                                 image_disk_format='qcow2',
                                 image_min_disk='1',
                                 image_min_ram='0',
                                 instance_type_ephemeral_gb='0',
                                 instance_type_flavorid='1',
                                 instance_type_id='2',
                                 instance_type_memory_mb='512',
                                 instance_type_name='m1.tiny',
                                 instance_type_root_gb='1',
                                 instance_type_rxtx_factor='1.0',
                                 instance_type_swap='0',
                                 instance_type_vcpu_weight=None,
                                 instance_type_vcpus='1'
                                 },
                             task_state='spawning',
                             terminated_at=None,
                             updated_at=2015-08-31T02:44:38Z,
                             user_data=u'<SANITIZED>,
                             user_id='ea4393b196684c8ba907129181290e8d',
                             uuid=92d22a62-c364-4169-9795-e5a34b5f5968,
                             vcpus=1,
                             vm_mode=None,
                             vm_state='building')
        :param image_meta: image object returned by nova.image.glance that
                           defines the image from which to boot this instance
                           e.g.
                           {
                               u'status': u'active',
                               u'deleted': False,
                               u'container_format': u'bare',
                               u'min_ram': 0,
                               u'updated_at': u'2015-08-17T07:46:48.708903',
                               u'min_disk': 0,
                               u'owner': u'52957ad92b2146a0a2e2b3279cdc2c5a',
                               u'is_public': True,
                               u'deleted_at': None,
                               u'properties': {},
                               u'size': 338735104,
                               u'name': u'emall-backend',
                               u'checksum': u'0f2294c98c7d113f0eb26ad3e76c86fa',
                               u'created_at': u'2015-08-17T07:46:20.581706',
                               u'disk_format': u'qcow2',
                               u'id': u'6004b47b-d453-4695-81be-cd127e23f59e'
                            }

        :param injected_files: User files to inject into instance.
        :param admin_password: Administrator password to set in instance.
        :param network_info:
           :py:meth:`~nova.network.manager.NetworkManager.get_instance_nw_info`
        :param block_device_info: Information about block devices to be
                                  attached to the instance.
        """

        bdms = block_device_info.get('block_device_mapping', [])
        if not instance.image_ref and len(bdms) > 0:
            volume_ids, bootable_volume_id = self._get_volume_ids_from_bdms(bdms)
            if bootable_volume_id:
                db_manager = DatabaseManager()
                cascaded_volume_id = db_manager.get_cascaded_volume_id(bootable_volume_id)
                # if cascaded volume already been created, then the data maybe changed, so cann't be created from image.
                if cascaded_volume_id:
                    LOG.info('Cascaded volume exist, need to transfer to image then create server from new image')
                    cascaded_backup_id = self.get_cascaded_volume_backup(bootable_volume_id)
                    # if cascaded_backup_id exist, means last time execute may fail, need to go on with last time job.
                    if cascaded_backup_id:
                        self.create_server_from_backup(cascaded_backup_id, bootable_volume_id, instance)
                    else:
                        cascaded_backup_id = self.create_backup_from_volume(self.project, bootable_volume_id, cascaded_volume_id)
                        self.create_server_from_backup(cascaded_backup_id, bootable_volume_id, instance)
                # if cascaded volume not exist, the data is the same between image and volume,
                # so we can create server from image.
                else:
                    LOG.info('Cascaded volume not exist, create server from image directly')
                    image_id = self._get_volume_source_image_id(context, bootable_volume_id)
                    instance.image_ref = image_id
                    self._spawn_from_image(context, instance, image_meta, injected_files,
                                            admin_password, network_info, block_device_info)
            else:
                raise Exception('No bootable volume for created server')
        else:
            self._spawn_from_image(context, instance, image_meta, injected_files,
              admin_password, network_info, block_device_info)

    def create_server_from_backup(self, cascaded_backup_id, cascading_volume_id, instance):
        cascaded_image_id = self.get_cascaded_backup_image(cascading_volume_id)
        # if cascaded_image_id exist, means last time it failed, need to go on.
        if cascaded_image_id:
            self.spawn_from_image_for_volume(instance, cascaded_image_id, cascading_volume_id)
        else:
            cascaded_image_id = self.create_image_from_backup(cascaded_backup_id, cascading_volume_id)
            self.spawn_from_image_for_volume(instance, cascaded_image_id, cascading_volume_id)

    def create_backup_from_volume(self, project_id, cascading_volume_id, cascaded_volume_id):
        create_backup_job_info = self.hws_client.vbs.create_backup(project_id, cascaded_volume_id)
        create_backup_job_detail_info = self._deal_with_job(create_backup_job_info, project_id,
                            self._after_create_backup_success, self._after_create_backup_fail,
                            cascading_volume_id=cascading_volume_id, cascaded_volume_id=cascaded_volume_id)
        if create_backup_job_info:
            backup_id = create_backup_job_detail_info['body']['entities']['backup_id']
        else:
            return Exception('Create tmp backup failed for cascading volume: %s' % cascading_volume_id)

        LOG.debug("Create tmp backup success for cascading volume: %s" % cascading_volume_id)
        return backup_id

    def create_image_from_backup(self, backup_id, cascading_volume_id):
        image_name = ''
        description = 'image for volume: %s' % ''
        job_info = self.hws_client.ims.create_image(image_name, description, backup_id)
        create_image_success_detail_info = self._deal_with_job(job_info, self._after_create_image_from_backup_success,
                                                               self._after_create_image_from_backup_fail,
                                                               cascading_volume_id=cascading_volume_id)
        if create_image_success_detail_info:
            # TODO, structure of image_create_response is need to check
            cascaded_image_id = create_image_success_detail_info['body']['entities']['image_id']
        else:
            raise Exception('Create tmp image failed for cascading volume: %s' % cascading_volume_id)

        LOG.debug('Create tmp image success for cascading volume: %s' % cascading_volume_id)

        return cascaded_image_id

    def _after_create_image_from_backup_success(self, detail_info, **kwargs):
        if kwargs:
            cascading_volume_id = kwargs['cascading_volume_id']
            # TODO, structure of image_create_response is need to check
            image_id = detail_info['body']['entities']['image_id']
            if image_id:
                db_manager = DatabaseManager()
                db_manager.update_cascaded_image_in_volume_mapping(cascading_volume_id, image_id)
                LOG.debug('update image_id: %s for cascading volume: %s' % (cascading_volume_id, image_id))
            else:
                error_info = 'Create image from backup failed. ERROR: %s' % json.dumps(detail_info)
                LOG.error(error_info)
                raise Exception(error_info)

    def _after_create_image_from_backup_fail(self, detail_info, **kwargs):
        cascading_volume_id = kwargs['cascading_volume_id']
        error_info = 'Create image from backup failed for cascading volume: %s. ERROR: %s' %\
                     (cascading_volume_id, json.dumps(detail_info))
        LOG.error(error_info)
        raise Exception(error_info)

    def get_cascaded_volume_backup(self, cascading_volume_id):
        db_manager = DatabaseManager()
        return db_manager.get_cascaded_backup(cascading_volume_id)

    def get_cascaded_backup_image(self, cascading_volume_id):
        db_manager = DatabaseManager()
        return db_manager.get_cascaded_backup_image(cascading_volume_id)

    def delete_cascaded_volume_backup_image(self, cascaded_volume_id):
        pass

    def _after_create_backup_success(self, create_backup_job_detail_info, **kwargs):
        if kwargs:
            cascading_volume_id = kwargs.get('cascading_volume_id')
            cascaded_volume_id = kwargs.get('cascaded_volume_id')
            backup_id = create_backup_job_detail_info['body']['entities']['backup_id']
            if backup_id:
                db_manager = DatabaseManager()
                db_manager.update_cascaded_backup_in_volume_mapping(cascading_volume_id, backup_id)
            else:
                raise Exception('Create backup failed: backup_id is None, error: %s' %
                                json.dumps(create_backup_job_detail_info))

    def _after_create_backup_fail(self, create_backup_job_detail_info, **kwargs):
        cascaded_volume_id = kwargs.get('cascaded_volume_id')
        error_info = 'HWS create backup failed for volume: %s Error, EXCEPTION: %s' % (cascaded_volume_id, json.dumps(create_backup_job_detail_info))
        LOG.error(error_info)
        raise Exception(error_info)

    def _deal_with_job(self, job_info, project_id,
                       function_deal_with_success=None,
                       function_deal_with_fail=None,
                       **kwargs):
        if job_info['status'] == 200:
            job_id = job_info['body']['job_id']
            job_detail_info = None

            while True:
                time.sleep(5)
                job_detail_info = self.hws_client.vbs.get_job_detail(project_id, job_id)
                if job_detail_info:
                    if job_detail_info['status'] == 200:
                        job_status = job_detail_info['body']['status']
                        if job_status == 'RUNNING':
                            LOG.debug('job<%s> is still RUNNING.' % job_id)
                            continue
                        elif job_status == 'FAIL':
                            if function_deal_with_fail:
                                function_deal_with_fail(job_detail_info, **kwargs)
                            error_info = 'job<%s> FAIL, ERROR INFO: %s' % (job_id, json.dumps(job_detail_info))
                            raise Exception(error_info)
                        elif job_status == 'SUCCESS':
                            if function_deal_with_success:
                                function_deal_with_success(job_detail_info, **kwargs)
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

        return job_detail_info

    def _get_volume_source_image_id(self, context, volume_id):
        """
        volume_image_metadata:
        {
            u'container_format': u'bare',
            u'min_ram': u'0',
            u'disk_format': u'qcow2',
            u'image_name': u'cirros',
            u'image_id': u'617e72df-41ba-4e0d-ac88-cfff935a7dc3',
            u'checksum': u'd972013792949d0d3ba628fbe8685bce',
            u'min_disk': u'0',
            u'size': u'13147648'
        }
        :param context:
        :param volume_id:
        :return:
        """
        volume_image_metadata = self.cinder_api.get_volume_image_metadata(context, volume_id)
        source_image_id = volume_image_metadata.get('image_id')
        return source_image_id


    def _get_volume_ids_from_bdms(self, bdms):
        """

        :param bdms:
         [{
            'guest_format': None,
            'boot_index': 0,
            'mount_device': u'/dev/sda',
            'connection_info': {
                u'driver_volume_type': u'vcloud_volume',
                'serial': u'ea552394-8308-4cce-824b-8fd9cc3be9d4',
                u'data': {
                    u'access_mode': u'rw',
                    u'qos_specs': None,
                    u'display_name': u'volume_01',
                    u'volume_id': u'ea552394-8308-4cce-824b-8fd9cc3be9d4',
                    u'backend': u'vcloud'
                }
            },
            'disk_bus': None,
            'device_type': None,
            'delete_on_termination': False
        }]
        :return: volume_ids, bootable_volume_id
        """
        volume_ids = []
        bootable_volume_id = None
        for bdm in bdms:
            volume_id = bdm['connection_info']['data']['volume_id']
            volume_ids.append(volume_id)
            if 0 == bdm['boot_index']:
                bootable_volume_id = volume_id

        return volume_ids, bootable_volume_id

    def _spawn_from_image(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        flavor = self._get_cascaded_flavor_id(instance)
        image_id = self._get_cascaded_image_id(instance)
        server_name = self._get_display_name(instance)

        vpc_id = CONF.hws.vpc_id
        subnet_id = CONF.hws.subnet_id
        subnet_id_list = [subnet_id]
        project_id = CONF.hws.project_id
        root_volume_type = "SATA"
        az = CONF.hws.resource_region
        try:
            created_job = self.hws_client.ecs.create_server(project_id, image_id, flavor,
                                                            server_name, vpc_id, subnet_id_list, root_volume_type,
                                                            availability_zone=az)
            job_status = created_job["status"]

            if job_status != 200:
                job_info = json.dumps(created_job)
                error_info = 'HWS Create Server Error, EXCEPTION: %s' % created_job
                raise Exception(error_info)

        except Exception:
            raise exception.VirtualInterfaceCreateException(traceback.format_exc())

        job_id = created_job['body']['job_id']

        def _wait_for_boot():
            """Called at an interval until the VM is running."""
            job_current_info = self.hws_client.ecs.get_job_detail(project_id, job_id)
            if job_current_info and job_current_info['status'] == 200:
                job_status_ac = job_current_info['body']['status']
                if job_status_ac == 'SUCCESS':
                    server_id = job_current_info['body']['entities']['sub_jobs'][0]["entities"]['server_id']
                    LOG.info('Add hws server id: %s' % server_id)
                    if server_id:
                        LOG.info('HWS add server id mapping, cascading id: %s, cascaded id: %s' %
                                 (instance.uuid, server_id))
                        db_manager = DatabaseManager()
                        db_manager.add_server_id_mapping(instance.uuid, server_id)
                        db_manager.add_server_id_name_mapping(instance.uuid, server_name)
                    else:
                        error_info = 'No server id found for cascading id: %s, server: %s' % (instance.uuid, server_name)
                        LOG.error(error_info)
                        raise Exception('HWS Create Server Error, EXCEPTION: %s' % error_info)
                    raise loopingcall.LoopingCallDone()
                elif job_status_ac == 'FAIL':
                    error_info = json.dumps(job_current_info)
                    LOG.error('HWS Create Server Error, EXCEPTION: %s' % error_info)
                    raise Exception(error_info)
                elif job_status_ac == "RUNNING":
                    LOG.debug('Job for creating server: %s is still RUNNING.' % server_name)
                    pass
                elif job_status_ac == "INIT":
                    LOG.debug('JOB for createing server: %s is init' % server_name)
                    pass
                else:
                    LOG.debug('JOB status is %s' % job_status_ac)
                    pass
            elif job_current_info and job_current_info['status'] == 'error':
                try:
                    self._deal_java_error(job_current_info)
                except Exception, e:
                    pass
            elif not job_current_info:
                pass
            else:
                error_info = json.dumps(job_current_info)
                # log.error('HWS Create Server Error, EXCEPTION: %s' % error_info)
                raise Exception(error_info)

        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_boot)
        timer.start(interval=5).wait()

    def spawn_from_image_for_volume(self, instance, image_id, cascading_volume_id):
        flavor = self._get_cascaded_flavor_id(instance)
        server_name = self._get_display_name(instance)

        vpc_id = CONF.hws.vpc_id
        subnet_id = CONF.hws.subnet_id
        subnet_id_list = [subnet_id]
        project_id = CONF.hws.project_id
        root_volume_type = "SATA"
        az = CONF.hws.resource_region
        cascading_server_id = instance.uuid
        db_manager = DatabaseManager()
        cascaded_server_id = db_manager.get_cascaded_server_id(cascading_server_id)
        if cascaded_server_id:
            info = 'HWS server for server: %s is already created, no need to create.' % instance.display_name
            LOG.debug(info)
        else:
            created_job = self.hws_client.ecs.create_server(project_id, image_id, flavor,
                                                                server_name, vpc_id, subnet_id_list, root_volume_type,
                                                                availability_zone=az)
            self._deal_with_job(created_job, project_id,
                                self._after_spawn_from_image_success,
                                self._after_spawn_from_image_fail,
                                instance=instance, cascading_volume_id=cascading_volume_id)

    def _after_spawn_from_image_fail(self, job_detail_info, **kwargs):
        instance = kwargs.get('instance')
        error_info = json.dumps(job_detail_info)
        LOG.error('HWS Create Server: %s Error, EXCEPTION: %s' % (instance.display_name, error_info))
        raise Exception(error_info)


    def _after_spawn_from_image_success(self, job_detail_info, **kwargs):
        instance = kwargs.get('instance')
        server_name = instance.display_name
        cascading_volume_id = kwargs.get('cascading_volume_id')
        db_manager = DatabaseManager()
        cascaded_backup_id = db_manager.get_cascaded_backup(cascading_volume_id)
        cascaded_image_id = db_manager.get_cascaded_backup_image(cascading_volume_id)

        server_id = job_detail_info['body']['entities']['sub_jobs'][0]["entities"]['server_id']
        LOG.info('Add hws server id: %s' % server_id)
        if server_id:
            LOG.debug('HWS add server id mapping, cascading id: %s, cascaded id: %s' %
                     (instance.uuid, server_id))
            db_manager.add_server_id_mapping(instance.uuid, server_id)
            db_manager.add_server_id_name_mapping(instance.uuid, server_name)
            if cascaded_backup_id:
                self._delete_cascaded_backup(cascaded_backup_id)
            if cascaded_image_id:
                self._delete_cascaded_image(cascaded_image_id)
        else:
            error_info = 'No server id found for cascading id: %s, server: %s' % (instance.uuid, server_name)
            LOG.error(error_info)
            raise Exception('HWS Create Server Error, EXCEPTION: %s' % error_info)

    def _delete_cascaded_backup(self, backup_id):
        job_delete_backup_info = self.hws_client.vbs.delete_backup(self.project, backup_id)
        try:
            job_delete_success_detail_info = self._deal_with_job(job_delete_backup_info, self.project)
        except Exception, e:
            #TODO: Add a backend job to scan failed delete task.
            error_info = 'Delete tmp backup: %s failed, Exception: %s' % (backup_id, traceback.format_exc())
            LOG.error(error_info)


    def _delete_cascaded_image(self, image_id):
        job_delete_image_info = self.hws_client.ims.delete_image(image_id)
        try:
            job_delete_image_detail_info = self._deal_with_job(job_delete_image_info, self.project)
        except Exception, e:
            #TODO: Add a backend job to scan failed delete task.
            error_info = 'Delete tmp image: %s failed, Exception: %s' % (image_id, traceback.format_exc())
            LOG.error(error_info)

    def _get_cascaded_flavor_id(self, instance):
        if instance.get('system_metadata'):
            cascading_flavor_id = instance.get('system_metadata').get('instance_type_flavorid')
            db_manager = DatabaseManager()
            cascaded_flavor_id = db_manager.get_cascaded_flavor_id(cascading_flavor_id)
            if cascaded_flavor_id:
                flavor = cascaded_flavor_id
            else:
                flavor = CONF.hws.flavor_id
        else:
            raise Exception('No system_metadata for instance: %s' % instance.display_name)
        LOG.debug('FLAVOR: %s' % flavor)

        return flavor

    def _get_cascaded_image_id(self, instance):
        cascading_image_id = instance.image_ref
        db_manager = DatabaseManager()
        cascaded_image_id = db_manager.get_cascaded_image_id(cascading_image_id)
        if cascaded_image_id:
            image_id = cascaded_image_id
        else:
            raise exception.NovaException('No matched image id in HWS cloud.')

        return image_id

    def _get_display_name(self, instance):
        original_display_name = instance.display_name
        if len(original_display_name) < 64:
            display_name = original_display_name
        else:
            display_name = self._get_random_name(8)

        return display_name

    def _get_random_name(self, lenth):
        return ''.join(random.sample(string.ascii_letters + string.digits, lenth))

    def attach_volume(self, context, connection_info, instance, mountpoint,
                    disk_bus=None, device_type=None, encryption=None):
        """


        :param context:
            ['auth_token',
            'elevated',
            'from_dict',
            'instance_lock_checked',
            'is_admin',
            'project_id',
            'project_name',
            'quota_class',
            'read_deleted',
            'remote_address',
            'request_id',
            'roles',
            'service_catalog',
            'tenant',
            'timestamp',
            'to_dict',
            'update_store',
            'user',
            'user_id',
            'user_name']
        :param connection_info:
            {
                u'driver_volume_type': u'vcloud_volume',
                'serial': u'824d397e-4138-48e4-b00b-064cf9ef4ed8',
                u'data': {
                    u'access_mode': u'rw',
                    u'qos_specs': None,
                    u'display_name': u'volume_02',
                    u'volume_id': u'824d397e-4138-48e4-b00b-064cf9ef4ed8',
                    u'backend': u'vcloud'
                }
            }
        :param instance:
        Instance(
            access_ip_v4=None,
            access_ip_v6=None,
            architecture=None,
            auto_disk_config=False,
            availability_zone='az01.hws--fusionsphere',
            cell_name=None,
            cleaned=False,
            config_drive='',
            created_at=2016-01-14T07: 17: 40Z,
            default_ephemeral_device=None,
            default_swap_device=None,
            deleted=False,
            deleted_at=None,
            disable_terminate=False,
            display_description='volume_backend_01',
            display_name='volume_backend_01',
            ephemeral_gb=0,
            ephemeral_key_uuid=None,
            fault=<?>,
            host='420824B8-AC4B-7A64-6B8D-D5ACB90E136A',
            hostname='volume-backend-01',
            id=57,
            image_ref='',
            info_cache=InstanceInfoCache,
            instance_type_id=2,
            kernel_id='',
            key_data=None,
            key_name=None,
            launch_index=0,
            launched_at=2016-01-14T07: 17: 43Z,
            launched_on='420824B8-AC4B-7A64-6B8D-D5ACB90E136A',
            locked=False,
            locked_by=None,
            memory_mb=512,
            metadata={

            },
            node='420824B8-AC4B-7A64-6B8D-D5ACB90E136A',
            numa_topology=<?>,
            os_type=None,
            pci_devices=<?>,
            power_state=0,
            progress=0,
            project_id='e178f1b9539b4a02a9c849dd7ea3ea9e',
            ramdisk_id='',
            reservation_id='r-marvoq8g',
            root_device_name='/dev/sda',
            root_gb=1,
            scheduled_at=None,
            security_groups=SecurityGroupList,
            shutdown_terminate=False,
            system_metadata={
                image_base_image_ref='',
                image_checksum='d972013792949d0d3ba628fbe8685bce',
                image_container_format='bare',
                image_disk_format='qcow2',
                image_image_id='617e72df-41ba-4e0d-ac88-cfff935a7dc3',
                image_image_name='cirros',
                image_min_disk='0',
                image_min_ram='0',
                image_size='13147648',
                instance_type_ephemeral_gb='0',
                instance_type_flavorid='1',
                instance_type_id='2',
                instance_type_memory_mb='512',
                instance_type_name='m1.tiny',
                instance_type_root_gb='1',
                instance_type_rxtx_factor='1.0',
                instance_type_swap='0',
                instance_type_vcpu_weight=None,
                instance_type_vcpus='1'
            },
            task_state=None,
            terminated_at=None,
            updated_at=2016-01-14T07: 17: 43Z,
            user_data=u'<SANITIZED>,
            user_id='d38732b0a8ff451eb044015e80bbaa65',
            uuid=9eef20f0-5ebf-4793-b4a2-5a667b0acad0,
            vcpus=1,
            vm_mode=None,
            vm_state='active')

        Volume object:
        {
            'status': u'attaching',
            'volume_type_id': u'type01',
            'volume_image_metadata': {
                u'container_format': u'bare',
                u'min_ram': u'0',
                u'disk_format': u'qcow2',
                u'image_name': u'cirros',
                u'image_id': u'617e72df-41ba-4e0d-ac88-cfff935a7dc3',
                u'checksum': u'd972013792949d0d3ba628fbe8685bce',
                u'min_disk': u'0',
                u'size': u'13147648'
            },
            'display_name': u'volume_02',
            'attachments': [],
            'attach_time': '',
            'availability_zone': u'az01.hws--fusionsphere',
            'bootable': True,
            'created_at': u'2016-01-18T07: 03: 57.822386',
            'attach_status': 'detached',
            'display_description': None,
            'volume_metadata': {
                u'readonly': u'False'
            },
            'shareable': u'false',
            'snapshot_id': None,
            'mountpoint': '',
            'id': u'824d397e-4138-48e4-b00b-064cf9ef4ed8',
            'size': 120
        }
        :param mountpoint: string. e.g. "/dev/sdb"
        :param disk_bus:
        :param device_type:
        :param encryption:
        :return:
        """
        cascading_volume_id = connection_info['data']['volume_id']
        db_manager = DatabaseManager()
        cascaded_volume_id = db_manager.get_cascaded_volume_id(cascading_volume_id)
        device_name = mountpoint
        cascaded_server_id = self._get_cascaded_server_id(instance)
        if not cascaded_server_id:
            raise Exception('No hws server mapping for cascading server: %s' % instance.uuid)

        if cascaded_volume_id:
            self._attach_volume(self.project, cascaded_server_id, cascaded_volume_id, device_name, cascading_volume_id)
        else:
            image_id = self._get_volume_source_image_id(context, cascading_volume_id)
            volume_obj = self.cinder_api.get(context, cascading_volume_id)
            size = volume_obj.get('size')
            volume_type = volume_obj.get('volume_type_id')

            if volume_type not in SUPPORT_VOLUME_TYPE:
                volume_type = SATA

            name = volume_obj.get('display_name')

            availability_zone = CONF.hws.resource_region

            cascaded_image_id = db_manager.get_cascaded_image_id(image_id)

            if cascaded_image_id:
                job_info = self.hws_client.evs.create_volume(self.project, availability_zone, size, volume_type,
                                                  name=name, imageRef=cascaded_image_id)
                job_detail_info = self._deal_with_job(job_info, self.project,
                                    self._after_create_volume_success,
                                    self._after_create_volume_fail, cascading_volume_id=cascading_volume_id)
                cascaded_volume_id = job_detail_info['body']['entities']['volume_id']
                self._attach_volume(self.project, cascaded_server_id, cascaded_volume_id, device_name, cascading_volume_id)

    def _attach_volume(self, project, server_id, volume_id, device_name, cascading_volume_id):
        """

        :param project: string, hws project id
        :param server_id: string, hws server id
        :param volume_id: string, hws volume id
        :param device_name: device name, e.g. '/dev/sdb'
        :param cascading_volume_id:  string, cascading volume id
        :return:
        """
        job_attach_volume = self.hws_client.ecs.attach_volume(project, server_id, volume_id, device_name)
        job_attach_volume_success_detail = self._deal_with_job(job_attach_volume, project)
        LOG.debug('Attach volume success for server: %s, volume: %s' % (server_id, cascading_volume_id))

    def _after_attach_volume_success(self, job_detail_info, **kwargs):
        pass

    def _after_attach_volume_fail(self, job_detail_info, **kwargs):
        pass

    def _after_create_volume_success(self, job_detail_info, **kwargs):
        cascading_volume_id = kwargs['cascading_volume_id']

        cascaded_volume_id = job_detail_info['body']['entities']['volume_id']
        db_manager = DatabaseManager()
        db_manager.add_volume_mapping(cascading_volume_id, cascaded_volume_id)
        LOG.debug('add volume mapping for cascading_volume_id: %s, cascaded_volume_id: %s' %
                  (cascading_volume_id, cascaded_volume_id))

    def _after_create_volume_fail(self, job_detail_info, **kwargs):
        cascading_volume_id = kwargs['cascading_volume_id']
        error_info = 'fail to create volume for cascading volume: %s, error: %s' %\
                     (cascading_volume_id, json.dumps(job_detail_info))
        LOG.error(error_info)
        raise Exception(error_info)

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None):
        """
        :param instance:
        :param network_info:
        :param block_device_info:
        :param destroy_disks:
        :param migrate_data:
        :return:
        """
        # TODO: currently, no matter the server is create by image or volume, delete directly.
        # But for server created by volume, some condition when delete server, the system volume need to remain.
        # so here need a TODO.
        try:
            cascading_server_id = instance.uuid
            db_manager = DatabaseManager()
            cascaded_server_id = db_manager.get_cascaded_server_id(cascading_server_id)
            if cascaded_server_id:
                project_id = CONF.hws.project_id
                cascaded_server_detail = self.hws_client.ecs.get_detail(project_id, cascaded_server_id)
                #{u'body': {u'itemNotFound': {u'message': u'Instance could not be found', u'code': 404}}, u'status': 404}
                if cascaded_server_detail['status'] == 404:
                    LOG.info('cascaded server is not exist in HWS, so return Delete Server SUCCESS.')
                    return

                delete_server_list = []
                delete_server_list.append(cascaded_server_id)
                delete_job_result = self.hws_client.ecs.delete_server(project_id, delete_server_list,
                                                                      True, True)
                self._deal_java_error(delete_job_result)
            else:
                # if there is no mapped cascaded server id, means there is no cascaded server
                # then we can directly return server deleted success.
                execute_info = "cascaded server is not exist for cascading id: , return delete success.%s" % cascading_server_id
                LOG.info(execute_info)

                return
        except Exception:
            raise exception.NovaException(traceback.format_exc())
        delete_job_id = delete_job_result['body']['job_id']

        def _wait_for_destroy():
            job_current_info = self.hws_client.ecs.get_job_detail(project_id, delete_job_id)
            if job_current_info and job_current_info['status'] == 200:
                job_status_ac = job_current_info['body']['status']
                if job_status_ac == 'SUCCESS':
                    db_manager = DatabaseManager()
                    db_manager.delete_server_id_by_cascading_id(cascading_server_id)
                    db_manager.delete_server_id_name_by_cascading_id(cascading_server_id)
                    raise loopingcall.LoopingCallDone()
                elif job_status_ac == 'FAIL':
                    error_info = json.dumps(job_current_info)
                    LOG.error('HWS Delete Server Error, EXCEPTION: %s' % error_info)
                    raise Exception(error_info)
                elif job_status_ac == "RUNNING":
                    LOG.debug('Job for delete server: %s is still RUNNING.' % cascading_server_id)
                    pass
                else:
                    raise Exception(job_current_info)
            elif job_current_info and job_current_info['status'] == 'error':
                try:
                    self._deal_java_error(job_current_info)
                except Exception, e:
                    # if it is java gateway error, we will always wait for it success.
                    # it maybe network disconnect error or others issue.
                    LOG.info('Java gateway issue, go on to wait for deleting server success.')
                    pass
            elif not job_current_info:
                pass
            else:
                error_info = json.dumps(job_current_info)
                LOG.error('HWS Delete Server Error, EXCEPTION: %s' % error_info)
                raise Exception(error_info)

        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_destroy)
        timer.start(interval=5).wait()

    def detach_volume(self, connection_info, instance, mountpoint,
                      encryption=None):
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
        cascading_volume_id = connection_info['data']['volume_id']
        db_manager = DatabaseManager()
        cascaded_volume = db_manager.get_cascaded_volume_id(cascading_volume_id)
        cascaded_server_id = db_manager.get_cascaded_server_id(instance.uuid)
        if not cascaded_server_id:
            error_info = 'Not exist cascaded server in hwclouds for server: %s.' % instance.uuid
            raise Exception(error_info)
        if cascaded_volume:
            job_detach_volume = self.hws_client.ecs.detach_volume(self.project, cascaded_server_id, cascaded_volume)
            self._deal_with_job(job_detach_volume, self.project)

    def after_detach_volume_success(self, job_detail_info, **kwargs):
        pass

    def after_detach_volume_fail(self, job_detail_info, **kwargs):
        pass

    def get_available_nodes(self, refresh=False):
        """Returns nodenames of all nodes managed by the compute service.

        This method is for multi compute-nodes support. If a driver supports
        multi compute-nodes, this method returns a list of nodenames managed
        by the service. Otherwise, this method should return
        [hypervisor_hostname].
        """
        hostname = socket.gethostname()
        return [hostname]

    def _get_host_stats(self, hostname):
        return {
            'vcpus': 32,
            'vcpus_used': 0,
            'memory_mb': 164403,
            'memory_mb_used': 69005,
            'local_gb': 5585,
            'local_gb_used': 3479,
            'host_memory_total': 164403,
            'disk_total':50000,
            'host_memory_free': 164403,
            'disk_used': 0,
            'hypervisor_type':'hws',
            'hypervisor_version':'5005000',
            'hypervisor_hostname':hostname,
            'cpu_info':'{"model": ["Intel(R) Xeon(R) CPU E5-2670 0 @ 2.60GHz"],'
                       '"vendor": ["Huawei Technologies Co., Ltd."], '
                       '"topology": {"cores": 16, "threads": 32}}',
            'supported_instances':jsonutils.dumps([["i686", "ec2", "hvm"], ["x86_64", "ec2", "hvm"]]),
            'numa_topology': None,
        }

    def get_available_resource(self, nodename):

        host_stats = self._get_host_stats(nodename)

        return {'vcpus': host_stats['vcpus'],
               'memory_mb': host_stats['host_memory_total'],
               'local_gb': host_stats['disk_total'],
               'vcpus_used': 0,
               'memory_mb_used': host_stats['host_memory_total'] -
                                 host_stats['host_memory_free'],
               'local_gb_used': host_stats['disk_used'],
               'hypervisor_type': host_stats['hypervisor_type'],
               'hypervisor_version': host_stats['hypervisor_version'],
               'hypervisor_hostname': host_stats['hypervisor_hostname'],
               'cpu_info': jsonutils.dumps(host_stats['cpu_info']),
               'supported_instances': jsonutils.dumps(
                   host_stats['supported_instances']),
               'numa_topology': None,
               }

    def get_info(self, instance):
        STATUS = power_state.NOSTATE

        try:
            cascaded_server_id = self._get_cascaded_server_id(instance)
            server = self.hws_client.ecs.get_detail(self.project, cascaded_server_id)

            if server and server['status'] == 200:
                hws_server_status = server['body']['server']['OS-EXT-STS:power_state']
                STATUS = HWS_POWER_STATE[hws_server_status]
                LOG.info('SYNC STATUS of server: %s, STATUS: %s' % (instance.display_name, hws_server_status) )
        except Exception:
            msg = traceback.format_exc()
            raise exception.NovaException(msg)
        return {'state': STATUS,
                'max_mem': 0,
                'mem': 0,
                'num_cpu': 1,
                'cpu_time': 0}

    def get_instance_macs(self, instance):
        """
        No need to implement.
        :param instance:
        :return:
        """
        pass

    def get_volume_connector(self, instance):
        pass

    def init_host(self, host):
        pass

    def list_instances(self):
        """List VM instances from all nodes."""
        instances = []
        project_id = CONF.hws.project_id
        list_result = self.hws_client.ecs.list(project_id)
        self._deal_java_error(list_result)
        servers = list_result['body']['servers']
        for server in servers:
            server_id = server['id']
            instances.append(server_id)

        return instances

    def power_off(self, instance, timeout=0, retry_interval=0):
        project_id = CONF.hws.project_id
        cascaded_server_id = self._get_cascaded_server_id(instance)
        if cascaded_server_id:
            stop_result = self.hws_client.ecs.stop_server(project_id, cascaded_server_id)
            self._deal_java_error(stop_result)
            LOG.info('Stop Server: %s, result is: %s' % (instance.display_name, stop_result))
        else:
            error_info = 'cascaded server id is not exist for cascading server: %s.' % instance.display_name
            LOG.error(error_info)
            raise exception.NovaException(error_info)

    def power_on(self, context, instance, network_info,
                 block_device_info=None):
        project_id = CONF.hws.project_id
        cascaded_server_id = self._get_cascaded_server_id(instance)
        if cascaded_server_id:
            start_result = self.hws_client.ecs.start_server(project_id, cascaded_server_id)
            self._deal_java_error(start_result)
            LOG.info('Start Server: %s, result is: %s' % (instance.display_name, start_result))
        else:
            error_info = 'cascaded server id is not exist for cascading server: %s.' % instance.display_name
            LOG.error(error_info)
            raise exception.NovaException(error_info)

    def reboot(self, context, instance, network_info, reboot_type,
               block_device_info=None, bad_volumes_callback=None):
        project_id = CONF.hws.project_id
        cascaded_server_id = self._get_cascaded_server_id(instance)
        if cascaded_server_id:
            reboot_result = self.hws_client.ecs.reboot_hard(project_id, cascaded_server_id)
            self._deal_java_error(reboot_result)
            LOG.info('Start Server: %s, result is: %s' % (instance.display_name, reboot_result))
        else:
            error_info = 'cascaded server id is not exist for cascading server: %s.' % instance.display_name
            LOG.error(error_info)
            raise exception.NovaException(error_info)

    def resume_state_on_host_boot(self, context, instance, network_info,
                                  block_device_info=None):
        pass

    def snapshot(self, context, instance, image_id, update_task_state):
        pass

    def _get_cascaded_server_id(self, instance):
        cascading_server_id = instance.uuid
        db_manager = DatabaseManager()
        cascaded_server_id = db_manager.get_cascaded_server_id(cascading_server_id)

        return cascaded_server_id

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
            raise exception.NovaException(error_info)