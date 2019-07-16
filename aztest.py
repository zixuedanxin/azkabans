#!/usr/bin/env python3

# import pandas as pd
# import os

# import argparse
from azkaban.azkaban import Flow, Project
from azkaban.Cookies import Cookies
from azkaban.utils import *
from azrun import *


def test_login():
    try:
        ck = Cookies()
        print("配置成功，获取到项目情况：", ck.fetch_projects())
    except Exception as e:
        print("配置失败", str(e))


if __name__ == '__main__':
    update_project("dw")

    # fl = Flow("dw", "end")
    # fl.execute()
    # print(get_prj_path("dw", file_type="dir", search_path="temp"))
    # ck = Cookies()
    # print(ck.get_execution_logs("2239"))
    # print(get_executor(active=0,rstype="port"))
    # pro = Properties('/home/xzh/mypython/etlpy/azkaban/azkabans/conf/dw/dw.properties')
    # pro.get_properties()
    # print(pro.properties.keys())
    # print(pro.get_value_by_key("end"))
    # print(get_project_info("dw", "cron"))
    # prj_prop=
    # fl.fetch_job()
    # fl.execute()
    # copy_dir(get_prj_path("dw", file_type="dir", search_path="temp"), '/home/xzh/dw', ignore_file_type=['job', 'flow'],only_file_type=['job', 'flow'])
    # schedule_project("dw", cron='0/10 5 20 6,7,8 * ?')
    # exec_project("dw")
    # print(get_project_info("cron"))
    # if zip_path:
    #     prj=Project("dw")
    #     prj.create_prj()
    #     prj.upload_zip(zip_path)
    # zipDir("/home/xzh/mypython/etlpy/azkaban/pyazkaban/temp/dw", "/home/xzh/mypython/etlpy/azkaban/pyazkaban/temp/dw.zip")
