# __author__: xuzh

from configparser import ConfigParser
import logging.config
import configparser
import os

main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 取文件的上上一级目录作为主目录
backup_path = os.path.join(main_path, 'bakup')  # 备份目录，每次上传新的zip文件先自动备份到该目录
temp_path = os.path.join(main_path, 'temp')  # 临时目录，生成job文件，并压缩zip文件然后上传
conf_path = os.path.join(main_path, 'conf')  # 配置文件目录，下面有全局配置文件config.ini,也有项目配置文件夹，每个文件夹作为一个项目
config_file = os.path.join(conf_path, "config.ini")  # 配置文件
logging.config.fileConfig(config_file)
logger = logging.getLogger()
config = configparser.ConfigParser()  # type: ConfigParser
config.read(config_file)
check_interval = config.get('base', 'check_interval')
azkaban_url = config.get('base', 'azkaban_url')  # type: str
login_user = config.get('base', 'login_user')  # type: str
login_pwd = config.get('base', 'login_pwd')  # type: str
