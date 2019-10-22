#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
import tempfile
from collections import namedtuple
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from ansible.inventory.manager import InventoryManager
from ansible.playbook.play import Play
from ansible.executor.playbook_executor import PlaybookExecutor
from ansible.executor.task_queue_manager import TaskQueueManager
from ansible.plugins.callback import CallbackBase
from ansible import constants as const
from .config import logger

const.HOST_KEY_CHECKING = False  # 统一设置不检查host_key


class AnsibleHost:
    def __init__(self, host, port=None, ssh_user=None, ssh_pass=None, connection='ssh'):
        self.host = host
        self.port = port
        self.ansible_connection = connection
        self.ansible_ssh_user = ssh_user
        self.ansible_ssh_pass = ssh_pass

    def __str__(self):
        result = 'ansible_ssh_host=' + str(self.host)
        if self.port:
            result += ' ansible_ssh_port=' + str(self.port)
        if self.ansible_connection:
            result += ' ansible_connection=' + str(self.ansible_connection)
        if self.ansible_ssh_user:
            result += ' ansible_ssh_user=' + str(self.ansible_ssh_user)
        if self.ansible_ssh_pass:
            result += ' ansible_ssh_pass=' + str(self.ansible_ssh_pass)
        return result


class AnsibleTaskResultCallback(CallbackBase):
    def __init__(self, display=None, option=None):
        super().__init__(display, option)
        self.result = None
        self.error_msg = None

    def v2_runner_on_ok(self, result):
        res = getattr(result, '_result')
        self.result = res
        self.error_msg = res.get('stderr')

    def v2_runner_on_failed(self, result, ignore_errors=None):
        if ignore_errors:
            return
        res = getattr(result, '_result')
        self.error_msg = res.get('stderr', '') + res.get('msg')

    def runner_on_unreachable(self, host, result):
        if result.get('unreachable'):
            self.error_msg = host + ':' + result.get('msg', '')

    def v2_runner_item_on_failed(self, result):
        res = getattr(result, '_result')
        self.error_msg = res.get('stderr', '') + res.get('msg')


class AnsibleTask:
    def __init__(self, hosts, extra_vars=None):
        self.hosts = hosts
        self._validate()
        self.hosts_file = None
        self._generate_hosts_file()
        Options = namedtuple('Options',
                             ['connection', 'module_path', 'forks', 'become', 'become_method', 'become_user', 'check',
                              'diff', 'host_key_checking', 'listhosts', 'listtasks', 'listtags', 'syntax'])

        self.options = Options(connection='ssh', module_path=None, forks=10,
                               become=None, become_method=None, become_user=None, check=False, diff=False,
                               host_key_checking=False, listhosts=None, listtasks=None, listtags=None, syntax=None)

        self.loader = DataLoader()
        self.passwords = dict(vault_pass='secret')
        self.inventory = InventoryManager(loader=self.loader, sources=[self.hosts_file])
        self.variable_manager = VariableManager(loader=self.loader, inventory=self.inventory)
        if extra_vars:
            self.variable_manager.extra_vars = extra_vars

    def _generate_hosts_file(self):
        self.hosts_file = tempfile.mktemp()
        with open(self.hosts_file, 'w+', encoding='utf-8') as file:
            hosts = []
            i_temp = 0
            for host in self.hosts:
                hosts.append('server' + str(i_temp) + ' ' + str(host))
                i_temp += 1
            file.write('\n'.join(hosts))

    def _validate(self):
        if not self.hosts:
            raise Exception('hosts不能为空')
        if not isinstance(self.hosts, list):
            raise Exception('hosts只能为list<AnsibleHost>数组')
        for host in self.hosts:
            if not isinstance(host, AnsibleHost):
                raise Exception('host类型必须为AnsibleHost')

    def exec_shell(self, command):
        source = {'hosts': 'all', 'gather_facts': 'no',
                  'tasks': [{'action': {'module': 'shell', 'args': command}, 'register': 'shell_out'}]
                  }
        play = Play().load(source, variable_manager=self.variable_manager, loader=self.loader)
        results_callback = AnsibleTaskResultCallback()
        tqm = None
        try:
            tqm = TaskQueueManager(
                inventory=self.inventory,
                variable_manager=self.variable_manager,
                loader=self.loader,
                options=self.options,
                passwords=self.passwords,
                stdout_callback=results_callback
            )
            tqm.run(play)
            if results_callback.error_msg:
                raise Exception(results_callback.error_msg)
            return results_callback.result
        except Exception as e:
            raise Exception(str(e))
        finally:
            if tqm is not None:
                tqm.cleanup()

    def exec_playbook(self, playbooks):
        results_callback = AnsibleTaskResultCallback()
        playbook = PlaybookExecutor(playbooks=playbooks,
                                    inventory=self.inventory,
                                    variable_manager=self.variable_manager,
                                    options=self.options,
                                    loader=self.loader,
                                    passwords=self.passwords)
        setattr(getattr(playbook, '_tqm'), '_stdout_callback', results_callback)
        playbook.run()
        if results_callback.error_msg:
            raise Exception(results_callback.error_msg)
        return results_callback.result

    def __del__(self):
        if self.hosts_file:
            os.remove(self.hosts_file)


class AnsibleAPI(object):
    """
    This is a General object for parallel execute modules.
    """

    def __init__(self, resource, *args, **kwargs):
        self.resource = resource
        self.inventory = None
        self.variable_manager = None
        self.loader = None
        self.options = None
        self.passwords = None
        self.callback = None
        self.__initializeData()
        self.results_raw = {}

    def __initializeData(self):
        """
        初始化ansible
        """
        Options = namedtuple('Options', ['connection', 'module_path', 'forks', 'timeout', 'remote_user',
                                         'ask_pass', 'private_key_file', 'ssh_common_args', 'ssh_extra_args',
                                         'sftp_extra_args',
                                         'scp_extra_args', 'become', 'become_method', 'become_user', 'ask_value_pass',
                                         'verbosity',
                                         'check', 'listhosts', 'listtasks', 'listtags', 'syntax'])

        # initialize needed objects
        self.variable_manager = VariableManager()
        self.loader = DataLoader()
        self.options = Options(connection='smart', module_path='/usr/local/bin/ansible', forks=100, timeout=10,
                               remote_user='root', ask_pass=False, private_key_file=None, ssh_common_args=None,
                               ssh_extra_args=None,
                               sftp_extra_args=None, scp_extra_args=None, become=None, become_method=None,
                               become_user='root', ask_value_pass=False, verbosity=None, check=False, listhosts=False,
                               listtasks=False, listtags=False, syntax=False)

        self.passwords = dict(sshpass=None, becomepass=None)
        self.inventory = InventoryManager(loader=self.loader,
                                          sources=self.resource)  # MyInventory(self.resource, self.loader, self.variable_manager).inventory
        self.variable_manager.set_inventory(self.inventory)

    def run(self, host_list, module_name, module_args):
        """
        run module from andible ad-hoc.
        module_name: ansible module_name
        module_args: ansible module args
        """
        # create play with tasks
        play_source = dict(
            name="Ansible Play",
            hosts=host_list,
            gather_facts='no',
            tasks=[dict(action=dict(module=module_name, args=module_args))]
        )
        play = Play().load(play_source, variable_manager=self.variable_manager, loader=self.loader)

        # actually run it
        tqm = None
        self.callback = AnsibleTaskResultCallback()
        try:
            tqm = TaskQueueManager(
                inventory=self.inventory,
                variable_manager=self.variable_manager,
                loader=self.loader,
                options=self.options,
                passwords=self.passwords,
            )
            tqm._stdout_callback = self.callback
            tqm.run(play)
        finally:
            if tqm is not None:
                tqm.cleanup()

    def run_playbook(self, host_list, role_name, role_uuid, temp_param):
        """
        run ansible palybook
        """
        try:
            self.callback = AnsibleTaskResultCallback()
            filenames = ['' + '/handlers/ansible/v1_0/sudoers.yml']  # playbook的路径
            template_file = ''  # 模板文件的路径
            if not os.path.exists(template_file):
                sys.exit()

            extra_vars = {}  # 额外的参数 sudoers.yml以及模板中的参数，它对应ansible-playbook test.yml --extra-vars "host='aa' name='cc' "
            host_list_str = ','.join([item for item in host_list])
            extra_vars['host_list'] = host_list_str
            extra_vars['username'] = role_name
            extra_vars['template_dir'] = template_file
            extra_vars['command_list'] = temp_param.get('cmdList')
            extra_vars['role_uuid'] = 'role-%s' % role_uuid
            self.variable_manager.extra_vars = extra_vars
            # actually run it
            executor = PlaybookExecutor(
                playbooks=filenames, inventory=self.inventory, variable_manager=self.variable_manager,
                loader=self.loader,
                options=self.options, passwords=self.passwords,
            )
            executor._tqm._stdout_callback = self.callback
            executor.run()
        except Exception as e:
            logger.error("error:", e.message)

    def get_result(self):
        self.results_raw = {'success': {}, 'failed': {}, 'unreachable': {}}
        for host, result in self.callback.host_ok.items():
            self.results_raw['success'][host] = result._result

        for host, result in self.callback.host_failed.items():
            self.results_raw['failed'][host] = result._result.get('msg') or result._result

        for host, result in self.callback.host_unreachable.items():
            self.results_raw['unreachable'][host] = result._result['msg']

        return self.results_raw
