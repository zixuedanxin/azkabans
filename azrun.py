#!/usr/bin/env python3

# import pandas as pd
# import os

import argparse
from azkaban.azkabans import Flow, Project
from azkaban.utils import *
from azkaban.azssh import restart_azkaban


def update_project(prj_nm):
    """
    更新项目的元数据，已经设置的计划会自动按新元数据执行
    :param prj_nm: 项目名称
    """
    zip_path = crt_job_file(prj_nm)
    if zip_path:
        prj = Project(prj_nm)
        prj.create_prj()  # 项目存在则不新建
        prj_old_flow_cnt = len(prj.fetch_flow())
        prj.upload_zip(zip_path)  # 上传zip文件
        prj_new_flow_cnt = len(prj.fetch_flow())
        shutil.rmtree(zip_path.replace(".zip", ""))  # 清空项目对应的临时目录，一般是temp目录下
        if prj_new_flow_cnt > 1:
            logger.warning(prj_nm + "一个项目出现多个工作流，不符合我们的业务规则。请以某个工作流或者end_flow作为结束")
        if 0 < prj_old_flow_cnt != prj_new_flow_cnt:
            logger.warning(prj_nm + "项目上传后工作流数量产生了变化，注意查看并确认是否修改定时任务")
        if prj_new_flow_cnt == 1 and len(prj.fetch_flow_schedule()) < 1:
            logger.warning("没有设置执行计划,将按照指定的文件配置设定执行计划")
            prj.schedule_flows()
    else:
        logger.error(prj_nm + " ： job文件生成失败或者压缩文件失败")


def schedule_project(prj_nm, flows=None, cron=None):
    """
    更新项目的元数据，已经设置的计划会自动按新元数据执行
    :param prj_nm: 是项目名字 如dw
    :param flows : 需要设置的工作流的，是个list类型，默认None就是给这个项目所有的工作流设执行计划
    :param cron: 是crom 的时间格式，默认的None就是从config读取配置时间.
    格式：秒 分 时 日 月 周 （周和日必须有个一是？无效,暂时不支持到秒级统一给0）
    """
    if prj_nm in get_projects():
        prj = Project(prj_nm)
        prj_new_flow_cnt = len(prj.fetch_flow())
        if prj_new_flow_cnt > 1:
            logger.warning(prj_nm + "一个项目出现多个工作流，不符合我们的业务规则。请以某个工作流或者end_flow作为结束")
        # if prj_new_flow_cnt == 1 and len(prj.fetch_flow_schedule()) < 1:
        logger.warning("设置执行计划,将按照指定的文件配置设定执行计划")
        prj.schedule_flows(cron=cron, flows=flows)
    else:
        logger.error(prj_nm + "项目还没有创建，没有找到相关信息")


def exec_project(prj_nm, flows=None, flow_override=None, disabled=None):
    """执行项目
    :param prj_nm: 是项目名字 如dw
    :param flows : 需要执行的工作流的，是个list类型，默认None就是给这个项目所有的工作流设执行
    :param flow_override: 是数据字典类，可以覆盖全局变量的参数，eg.{"etl_dt":'2019-07-18'}
    :param disabled: job name 的list类型，选择跳过哪些job,eg.['start','el_crm']
    """
    logger.info("prj_nm={0} flows={1} flow_override={2} disabled={3}".format(prj_nm, flows, flow_override, disabled))
    if prj_nm in get_projects():
        prj = Project(prj_nm)
        all_flows = prj.fetch_flow()
        if flows is None:
            flows = all_flows
        for f in all_flows:
            if f in flows:
                fl = Flow(prj_nm, f, disabled=disabled, flow_override=flow_override)
                fl.execute()
    else:
        logger.error(prj_nm + "项目还没有创建，没有找到相关信息")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="远程部署azkaban")
    helps = "u 表示更新项目元数据， e 表示执行项目 s 表示给项目添加执行计划,a 激活执行节点 r 重启azkaban"
    parser.add_argument("action", type=str, choices=["u", "e", "s", "a", "r"], help=helps)
    parser.add_argument("prj_nm", type=str, help="项目名称字符型", default="dw")
    parser.add_argument("-f", "--flows", help="工作流的字符列表,eg: \"['a','b']\" ", type=str, default=None)
    parser.add_argument("-t", "--crontab", help="cron的定时器格式字符串", type=str, default=None)
    parser.add_argument("-i", "--ignore", help="job name的字符列表 eg.\"['a','b']\" ", type=str, default=None)
    parser.add_argument("-p", "--param", help="参数传入，数据字典,可以覆盖全局参数 \"{'s':1}\"", type=str, default=None)
    args = parser.parse_args()
    action = args.action
    project = args.prj_nm
    flows_list = eval_str(args.flows)  # help="工作流的字符列表,eg: \"['a','b']\"
    ignore = eval_str(args.ignore)  # help="job name的字符列表 eg.\"['a','b']\" "
    param = eval_str(args.param)  # "参数传入，数据字典,可以覆盖全局参数 \"{'s':1}\""
    if action == "u":
        # 更新元数据
        update_project(project)  # 上传新项目后，会自动加入定时任务。如果有特殊需求只发布不加定时任务的。需要手动删除
    elif action == "e":
        # 执行工作流
        if param is None or type(param) == dict:
            exec_project(project, flows=flows_list, flow_override=param, disabled=ignore)
    elif action == "s":
        # 设置执行计划
        schedule_project(project, flows=flows_list, cron=args.crontab)
    elif action == "a":
        # 激活所有的执行节点
        if project == "all":
            active_executor()
        else:
            active_executor(project, port=12321)
    elif action == "r":
        # 设置执行计划
        if project == "all":
            restart_azkaban()
        else:
            restart_azkaban()
