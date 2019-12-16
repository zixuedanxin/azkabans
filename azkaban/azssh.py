# 远程ssh命令操作 功能待定
from azkaban.config import logger
import os
import time
from azkaban.utils import active_executor
import subprocess

azkaban_root_path = "/opt/softs/azkaban"


def exec_shell(shell, logs_print=True):
    logger.info("执行shell:" + shell)
    try:
        proc = subprocess.Popen(shell, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)  # subprocess.STDOUT
        pre_line = ''
        error_line = ''
        while proc.poll() is None:
            line = proc.stdout.readline().strip().decode('utf-8')
            if line and len(line) > 2:
                pre_line = line
                if 'error' in line.lower():
                    error_line = line
                if logs_print:
                    logger.info(line)
        # outs, errs = proc.communicate(timeout=15)
        # res是一个对象，需要读取res对象的stderr\strout\stdin的属性，才可获取值
        # 如果：err有值，即表示命令执行报错，即：stdout就为空
        if proc.returncode == 0:
            logger.info('shell 执行成功')
            return True
        else:
            raise Exception(error_line + '\n' + pre_line)
    except Exception as e:
        logger.error('shell执行失败：' + str(e))
        raise Exception("shell执行失败:" + str(e))


def restart_azkaban(azkaban_path="/opt/softs/azkaban"):
    """
    启动或重启azkaban
    :param azkaban_path: azkaban安装路径（目录下有web-server和exec-server目录）
    :return:
    """
    global exec_server, web_server
    os.chdir(azkaban_path)
    # print(os.getcwd())
    dirs = os.listdir(azkaban_path)
    for i in dirs:
        if 'web-' in i.lower() or '-web' in i.lower():
            web_server = os.path.join(azkaban_path, i)
            continue
        if 'exec-' in i.lower() or '-exec' in i.lower():
            exec_server = os.path.join(azkaban_path, i)
            continue
    os.chdir(exec_server)
    tp = os.popen("jps")  # 判断是否已经启动
    jps = tp.readlines()
    tp.close()
    azkaban_exec = False
    azkaban_web = False
    for i in jps:
        tp = i.strip().split(" ")
        if tp[1].strip().upper() == 'AzkabanExecutorServer'.upper():
            azkaban_exec = True
        if tp[1].strip().upper() == 'AzkabanWebServer'.upper():
            azkaban_web = True
    if azkaban_exec:
        exec_shell("bin/shutdown-exec.sh")
    exec_shell("bin/start-exec.sh")
    time.sleep(10)
    active_executor()
    os.chdir(web_server)
    time.sleep(10)
    logger.info(os.getcwd())
    if azkaban_web:
        exec_shell("bin/shutdown-web.sh")
    exec_shell("bin/start-web.sh")


# def start_azkaban(azkaban_path="/opt/softs/azkaban"):
#     global exec_server, web_server
#     os.chdir(azkaban_path)
#     # print(os.getcwd())
#     dirs = os.listdir(azkaban_path)
#     for i in dirs:
#         if 'web-' in i.lower() or '-web' in i.lower():
#             web_server = os.path.join(azkaban_path, i)
#             continue
#         if 'exec-' in i.lower() or '-exec' in i.lower():
#             exec_server = os.path.join(azkaban_path, i)
#             continue
#     os.chdir(exec_server)
#     # exec_shell("bin/shutdown-exec.sh")
#     exec_shell("bin/start-exec.sh")
#     time.sleep(10)
#     active_executor()
#     os.chdir(web_server)
#     time.sleep(10)
#     logger.info(os.getcwd())
#     # exec_shell("bin/shutdown-web.sh")
#     exec_shell("bin/start-web.sh")


if __name__ == '__main__':
    pass
