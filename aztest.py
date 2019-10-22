#!/usr/bin/env python3

# import pandas as pd
# import os

# import argparse
from azkaban.config import *
from azkaban.azkabans import Flow, Project
from azkaban.cookies import Cookies
from azkaban.utils import *
from azkaban.azssh import exec_shell
from azrun import *
import sys


def login_test():
    try:
        ck = Cookies()
        print("配置成功，获取到项目情况：", ck.fetch_projects())
    except Exception as e:
        print("配置失败", str(e))


def pre_run_prj(prj_nm, start_index=0, my_prop=None):
    prj_conf_path = os.path.join(conf_path, prj_nm)
    filepath = os.path.join(prj_conf_path, prj_nm + '.csv')
    global_prop = get_global_prop(prj_nm)
    global_prop.update(my_prop)
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        df['command'] = df['command'].fillna("echo  batch_dt:${batch_dt}")
        for i in df.index:
            if i >= start_index:
                check_cmd = rplc_cmd_with_prop(df.loc[i, 'command'], prj_nm, global_prop)
                try:
                    rs = exec_shell(check_cmd, logs_print=False)
                    df.loc[i, 'run_rs'] = str(rs)
                except Exception as e:
                    df.loc[i, 'run_rs'] = str(e)
        df.to_csv(temp_path + '/%s.csv' % (prj_nm,), index=False)
        logger.info("结果输出到temp_path + '/%s.csv" % (prj_nm,))


if __name__ == '__main__':
    update_project("dw")
    # pre_run_shell()
    #pre_run_prj("dw_daily", 15, my_prop={'etl_home': '/home/xzh/dps/etl/dpsetl'})
    # exec_shell("python3 /home/xzh/dps/etl/dpsetl/dpsetl/dw/dim_emp.py 2019-10-22",logs_print=True)

