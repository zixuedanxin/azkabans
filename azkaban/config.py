# __author__: xuzh
import sys
from configparser import ConfigParser
import logging.config
import configparser
import os
import time

main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 取文件的上上一级目录作为主目录
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
conf_path = os.path.join(main_path, 'conf')  # 配置文件目录，下面有全局配置文件config.ini,也有项目配置文件夹，每个文件夹作为一个项目
config_file = os.path.join(conf_path, "config.ini")  # 配置文件
logging.config.fileConfig(config_file)
logger = logging.getLogger()
config = configparser.ConfigParser()  # type: ConfigParser
config.read(config_file)  # 取数参数配置文件
check_interval = config.get('DEFAULT', 'check_interval')
is_prod = eval(config.get('DEFAULT', 'is_prod'))  # 是否生成环境
azkaban_url = config.get('base', 'azkaban_url')  # type: str
login_user = config.get('base', 'login_user')  # type: str
login_pwd = config.get('base', 'login_pwd')  # type: str
backup_path = os.path.join(main_path, 'bakup')  # 备份目录，每次上传新的zip文件先自动备份到该目录
temp_path = os.path.join(main_path, 'temp')  # 临时目录，生成job文件，并压缩zip文件然后上传
backup_path = config.get('base', 'backup_path', fallback=backup_path)  # 如果config.ini有配置则从配置中获取
temp_path = config.get('base', 'temp_path', fallback=temp_path)  # 如果config.ini有配置则从配置中获取
check_file_exists = config.get('DEFAULT', 'check_file_exists', fallback=False)
job_config_keys = ['type', 'flow.name', 'description', 'retries', 'retry.backoff', "working.dir", 'failure.emails', 'notify.emails',
                   'success.emails', 'command']  # job参数
# logger.warning("配置文件config.ini路径："+config_file)
azkaban_config_param = {
    'azkaban.job.attempt': 'azkaban.job.attempt',  # job重试次数，从0开始增加
    'azkaban.job.id': 'azkaban.job.id',  # 运行的job name
    'azkaban.flow.flowid': 'azkaban.flow.flowid',  # 运行的job的flow name
    'azkaban.flow.execid': 'azkaban.flow.execid',  # flow的执行id
    'azkaban.flow.projectid': 'azkaban.flow.projectid',  # 工程id
    'azkaban.flow.projectname': 'azkaban.flow.projectname',  # 工程名称
    'azkaban.flow.projectversion': 'azkaban.flow.projectversion',  # project上传的版本
    'azkaban.flow.uuid': 'azkaban.flow.uuid',  # flow uuid
    'azkaban.flow.start.timestamp': 'azkaban.flow.start.timestamp',  # flow start的时间戳
    'azkaban.flow.start.year': 'azkaban.flow.start.year',  # flow start的年份
    'azkaban.flow.start.month': 'azkaban.flow.start.month',  # flow start 的月份
    'azkaban.flow.start.day': 'azkaban.flow.start.day',  # flow start 的天
    'azkaban.flow.start.hour': 'azkaban.flow.start.hour',  # flow start的小时
    'azkaban.flow.start.minute': 'azkaban.flow.start.minute',  # start 分钟
    'azkaban.flow.start.second': 'azkaban.flow.start.second',  # start 秒
    'azkaban.flow.start.millseconds': 'azkaban.flow.start.millseconds',  # start的毫秒
    'azkaban.flow.start.timezone': 'azkaban.flow.start.timezone',  # start 的时区
    'retries': 'retries',  # 失败的job的自动重试的次数 job参数
    'retry.backoff': 'retry.backoff',  # 重试的间隔（毫秒）
    'working.dir': 'working.dir',  # 可以重新指定任务执行的工作目录，默认为目前正在运行的任务的工作目录
    'env.property': 'env.property',  # 指定在命令执行前需设置的环境变量。Property定义环境变量的名称，job参数
    'failure.emails': 'failure.emails',  # job失败时发送的邮箱,用逗号隔开 job参数
    'success.emails': 'success.emails',  # job成功时发送的邮箱，用逗号隔开 job参数
    'notify.emails': 'notify.emails',  # job成功或失败都发送的邮箱，用逗号隔开 job参数
    'JOB_PROP_FILE': "",
    'JOB_OUTPUT_PROP_FILE': "",
    'main_path': main_path,
    'etl_dt': time.strftime("%Y-%m-%d"),
    'etl_dtm': time.strftime("%Y-%m-%d %H:%M:%S"),
    'etl_tm': time.strftime("%H:%M:%S"),
    'batch_id': time.strftime("%Y%m%d%H%M%S"),
    'batch_dt': time.strftime("%Y-%m-%d"),
}


def get_executor_config():
    executor_config = {}
    for i in config["executor"]:
        if "." in i:
            i = i.lower()
            pos = i.rfind(".")
            host_nm = i[0:pos]
            if host_nm not in executor_config:
                executor_config[host_nm] = {}
            param = i[pos + 1:]
            if param == 'password':
                param = 'pwd'
            executor_config[host_nm][param] = config["executor"][i]
    return executor_config

# if __name__ == "__main__":
#     print(get_executor_config(),check_file_exists)
