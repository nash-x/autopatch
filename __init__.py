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
        self.current_dir = os.path.split(os.path.realpath(__file__))[0]
        self.patches_dir = os.sep.join([self.current_dir, 'patches'])
        self.back_time = datetime.datetime.now()
        self.backup_dir = os.sep.join([self.current_dir, 'backup', str(self.back_time)])


    def execute_patch(self):
        Printer.print_string('START to patch..', Printer.HEADER_1)
        patch_dirs_list = self.get_patch_dir_list()
        self._validate_if_files_conflict(patch_dirs_list)
        self._backup_all_patches(patch_dirs_list)
        self._install_all_patches(patch_dirs_list)
        Printer.print_string('END to patch!!!', Printer.HEADER_1)

    def _validate_if_files_conflict(self, patch_dirs_list):
        conflict_files = self._get_patch_file_conflict(patch_dirs_list)
        if conflict_files:
            Printer.print_string('Patches Conflit Validate FAILED, There are some conflict files.', Printer.HEADER_1)
            Printer.print_string('Conflict Patches', Printer.HEADER_2)
            Printer.print_list(conflict_files.keys(), Printer.TEXT_1)
            Printer.print_string('Conflict Patches Details', Printer.HEADER_2)
            for conflict_patch_name_mapping, files in conflict_files.items():
                Printer.print_string(conflict_patch_name_mapping, Printer.TEXT_1)
                Printer.print_list(files, Printer.TEXT_2)
            exit(1)
        else:
            Printer.print_string('Validate Patches Conflict Pass. There are no Patches Conflict.', Printer.HEADER_1)

    def _get_patch_file_conflict(self, patch_dirs_list):
        conflict_result = {}
        patch_map_files = {}
        for patch in patch_dirs_list:
            files = self.get_files(patch, None)
            patch_map_files[patch] = [relative_path for absolute_path, relative_path in files]
        checked_patch_list = []
        for check_patch, check_files in patch_map_files.items():
            checked_patch_list.append(check_patch)
            for patch, files in patch_map_files.items():
                if patch in checked_patch_list:
                    continue
                else:
                    conflict_files = self._get_same_items(check_files, files)
                    if conflict_files:
                        conflict_patches = ' : '.join([os.path.split(check_patch)[1], os.path.split(patch)[1]])
                        conflict_result[conflict_patches] = conflict_files
                    else:
                        continue

        return conflict_result

    def _get_same_items(self, set_a, set_b):
        return [item for item in set_a if item in set_b]

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
        if not os.path.exists(self.backup_dir):
            os.mkdir(self.backup_dir)

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

class Printer(object):

    HEADER_1 = '**%s'
    HEADER_2 = '****%s'
    TEXT_1 = '********%s'
    TEXT_2 = '************%s'

    FORMAT = [HEADER_1, HEADER_2, TEXT_1, TEXT_2]

    @staticmethod
    def print_list(aim_list, printer_format):
        if printer_format not in Printer.FORMAT:
            print(aim_list)
        for each_line in aim_list:
            print(printer_format % each_line)

    @staticmethod
    def print_string(content, printer_format):
        if printer_format not in Printer.FORMAT:
            print(content)
        else:
            print(printer_format % content)

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