__author__ = 'Administrator'
import os
import shutil
import sys
import datetime
import errno
from distutils import dir_util


class AutoPatch(object):

    def __init__(self):
        self.openstack_path = get_openstack_installed_path()
        self.openstack_path = '/home/nash/site-package/'
        self.current_dir = os.path.split(os.path.realpath(__file__))[0]
        self.patches_dir = os.sep.join([self.current_dir, 'patches'])
        self.back_time = datetime.datetime.now()
        self.backup_dir = os.sep.join([self.current_dir, 'backup', str(self.back_time)])
        if not os.path.exists(self.backup_dir):
            os.mkdir(self.backup_dir)


    def execute_patch(self):
        patch_dirs_list = self.get_patch_dir_list()
        self._backup_all_patches(patch_dirs_list)
        self._install_all_patches(patch_dirs_list)

    def _install_all_patches(self, patch_full_dirs_list):
        for patch_full_dir in patch_full_dirs_list:
            self._install_one_patch(patch_full_dir)

    def _install_one_patch(self, patch_full_dir):
        patch_module_dirs = os.listdir(patch_full_dir)
        for patch_module_name in patch_module_dirs:
            source_module_dir = os.sep.join([patch_full_dir, patch_module_name])
            sitepackage_module_dir = os.sep.join([self.openstack_path, patch_module_name])
            dir_util.copy_tree(source_module_dir, sitepackage_module_dir)

    def _backup_all_patches(self, patch_dirs_list):

        for patch_dir in patch_dirs_list:
            self._backup_one_patch(patch_dir)

    def _backup_one_patch(self, patch_dir):
        patch_name = os.path.split(patch_dir)[1]
        patch_backup_dir = os.sep.join([self.backup_dir, patch_name])
        patch_module_dirs = os.listdir(patch_dir)

        for patch_module_name in patch_module_dirs:
            module_backup_dir = os.sep.join([patch_backup_dir, patch_module_name])
            sitepackage_module_dir = os.sep.join([self.openstack_path, patch_module_name])

            if os.path.exists(sitepackage_module_dir):
                backup = Backup(sitepackage_module_dir, module_backup_dir)
                backup.execute_backup()

    def get_patch_dir_list(self):
        """
        To get patch full directory list.
        :return: list, full directory list of patches
        """
        full_patch_dirs = []
        patch_dirs = os.listdir(self.patches_dir)
        for patch_dir in patch_dirs:
            full_patch_dir = os.sep.join([self.patches_dir, patch_dir])
            if os.path.isdir(full_patch_dir):
                full_patch_dirs.append(full_patch_dir)
        return full_patch_dirs

    def get_files(self, specified_path, filters):
        """

        :param path, absolute path
        :param filters: array, specified valid file suffix.
        :return:
        for example:
        [(/root/tricircle-master/novaproxy/nova/compute/clients.py,
                nova/compute/clients.py), ..]
        """
        files_path = []
        file_sys_infos = os.walk(specified_path)

        for (path, dirs, files) in file_sys_infos:
            if not filters:
                for file in files:
                    absolute_path = os.path.join(path, file)
                    relative_path = absolute_path.split(specified_path)[1].split(os.path.sep, 1)[1]
                    files_path.append((absolute_path, relative_path))
            else:
                for file in files:
                    if os.path.splitext(file)[1] in filters:
                        absolute_path = os.path.join(path, file)
                        relative_path = absolute_path.split(specified_path)[1].split(os.path.sep, 1)[1]
                        files_path.append((absolute_path, relative_path))
                    else:
                        continue
        return files_path

class Backup(object):
    def __init__(self, backup_file, backup_dir):
        """

        :param backup_file: string, source full directory which need to be backup
        :param backup_dir: string, full backup directory, which should be not exist.
        :return:
        """
        self.backup_file = backup_file
        self.backup_dir = backup_dir

    def copy_anything(self, src, dst):
        try:
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns('*.pyc'))
        except OSError as exc:
            if exc.errno == errno.ENOTDIR:
                shutil.copy(src, dst)
            else:
                raise

    def execute_backup(self):
        self.copy_anything(self.backup_file, self.backup_dir)

def get_openstack_installed_path():
    paths = [path for path in sys.path if 'site-packages' in path and 'local' not in path]
    if not paths:
        return None
    else:
        openstack_installed_path = paths[0]
        return openstack_installed_path

if __name__ == '__main__':
    autopatch = AutoPatch()
    autopatch.execute_patch()