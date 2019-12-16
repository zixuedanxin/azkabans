# __author__: xuzh
import time
import pymysql
import pandas as pd
from azkaban.cron import Cron
import os
import zipfile
import re
import requests
import shutil
from azkaban.properties import Properties
from six.moves.urllib.parse import urlparse
from azkaban.config import main_path, logger, conf_path, azkaban_url, config, temp_path, check_file_exists, job_config_keys, \
    azkaban_config_param, is_prod
import sys


def eval_str(strs):
    if strs:
        try:
            rs = eval(strs)
            return rs
        except Exception as e:
            logger.error("参数格式不对 -f/-i/-d \"['a','b']\" -p \"{'s':1}\"  :" + str(e))
            sys.exit(0)
    else:
        return None


def parse_url(url):
    """
    解析URL, 返回 tuple of (username, password, address)
    :param url: HTTP endpoint (including protocol, port, and optional user /password).
    Supported url formats:
    + protocol://host:port
    + protocol://user@host:port
    + protocol://user:password@host:port
    + user@protocol://host:port (compatibility with older versions)
    + user:password@protocol://host:port (compatibility with older versions)
    """
    if not re.match(r'[a-zA-Z]+://', url) and not re.search(r'@[a-zA-Z]+://', url):
        # no scheme specified, default to http://
        url = 'http://' + url
    if re.search(r'@[a-zA-Z]+://', url):
        # compatibility mode: `user@protocol://host:port` or
        # `user:password@protocol://host:port`
        splitted = url.rstrip('/').split('@')
        if len(splitted) == 1:
            address = splitted[0]
            user = None
            password = None
        elif len(splitted) == 2:
            address = splitted[1]
            creds = splitted[0].split(':', 1)
            if len(creds) == 1:
                user = creds[0]
                password = None
            else:
                user, password = creds
        else:
            raise Exception('Malformed url: %r' % (url,))
        return user, password, address
    else:
        parsed = urlparse(url)
        return (parsed.username, parsed.password,
                '%s://%s:%s' % (parsed.scheme, parsed.hostname, parsed.port))


def azkaban_request(method, url=azkaban_url, **kwargs):
    """
    azkaban api 请求request函数.
    :param method: GET, POST, etc.
    :param url:
    """
    try:
        response = requests.request(url=url, method=method, **kwargs)
    except requests.ConnectionError as err:
        raise Exception('Unable to connect to Azkaban server %r: %s', url, err)
    except requests.exceptions.MissingSchema:
        raise Exception('Invalid Azkaban server url: %r.', url)
    else:
        return response


# # 通过校验MD5 来判别文件是否变动
def get_md5(file_path):
    files_md5 = os.popen('md5 %s' % file_path).read().strip()
    file_md5 = files_md5.replace('MD5 (%s) = ' % file_path, '')
    return file_md5


def copy_dir(source_path, target_path, ignore_file_type=None, only_file_type=None):
    """
    only_file_type 和 ignore_file_type 不能同时用
    :param source_path:
    :param target_path:
    :param ignore_file_type: 忽略的文件类型
    :param only_file_type: 只复制的文件类型
    :return:
    """
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    for files in os.listdir(source_path):
        name = os.path.join(source_path, files)
        back_name = os.path.join(target_path, files)
        if os.path.isfile(name):
            filetype = files.split(".")[-1]
            copy_flag = True
            if ignore_file_type and filetype in ignore_file_type:
                copy_flag = False
            if only_file_type is None:
                only_flag = True
            else:
                if filetype in only_file_type:
                    only_flag = True
                else:
                    only_flag = False
            if copy_flag and only_flag:
                shutil.copy(name, back_name)
                # if os.path.isfile(back_name):
                #     #i f get_md5(name) != get_md5(back_name):  # 通过校验MD5 来判别文件是否变动
                #     shutil.copy(name, back_name)
                # else:
                #     shutil.copy(name, back_name)
        else:
            if not os.path.isdir(back_name):
                os.makedirs(back_name)
            copy_dir(name, back_name, ignore_file_type, only_file_type)


def active_executor(hosts=None, port=12321):
    """激活各节点你的Executor"""
    unactive_host_list = get_executor(active=0)
    if len(unactive_host_list) < 1:
        logger.info("所有节点都已经激活")
        return

    if hosts:
        if hosts not in unactive_host_list:
            logger.info("{0}节点没有部署成功，请确认是否部署成功和hostname".format(hosts))
            return
        url = "http://{0}:{1}/executor?action=activate".format(hosts, port)
        try:
            rs = requests.get(url)
            if str(rs.content, 'utf-8') == "{\"status\":\"success\"}":
                logger.info("Executor : {0} 激活成功".format(hosts))
            else:
                logger.error("激活Executor失败,请确认Executor正确启动")
        except Exception as e:
            logger.error("激活Executor失败,请确认正确的ip和port" + str(e))
    else:
        rs = get_executor(active=0, rstype="port")
        for i in rs.keys():
            active_executor(i, rs[i])


def get_str_set(job_status_dict):
    """
    把job join到一起
    :param job_status_dict:
    :return:
    """
    union_set = job_status_dict.get('SUCCEEDED', set()).union(job_status_dict.get('SKIPPED', set()))
    return "[\"" + "\",\"".join(union_set) + "\"]"


def get_current_timekey():
    """
    当前时间
    :return:
    """
    return time.strftime("%Y%m%d%H%M%S")


def azkaban_mysql_conn():
    """
    azkaban s数据库连接
    :return:
    """
    con = pymysql.connect(
        host=config.get('base', 'mysql_host'),
        port=config.getint('base', 'mysql_port'),
        user=config.get('base', 'mysql_user'),
        password=config.get('base', 'mysql_pwd'),
        database=config.get('base', 'mysql_db'),
        charset="utf8")
    return con


def azkaban_blob_to_str(blobs):
    """
    将azkaban mysql中的longblob转化成字符串
    :param blobs:
    :return:
    """
    import gzip
    data = gzip.decompress(blobs).decode("utf-8")
    return data


def get_projects():
    """
    获取项目列表
    :return:
    """
    conn = azkaban_mysql_conn()
    df = pd.read_sql("select id prj_id,name prj_nm from projects where active=1", conn)
    st = df.groupby('prj_nm').count()
    st = st[st.prj_id > 1]
    if st.shape[0] > 0:
        raise Exception("项目名称出现重复的，不符合规则: " + ",".join(list(st.index)))
    df = df.set_index(keys='prj_nm')
    conn.close()
    return df.to_dict()['prj_id']


def get_execution_prj_and_flow(exec_id):
    """
    查找execu_id对应的项目和flow名称
    :param exec_id:
    :return:
    """
    conn = azkaban_mysql_conn()
    sql = """select exec_id,flow_id,p.name prj_nm from execution_flows f 
        left join projects p on f.project_id=p.id
        where exec_id={0} limit 1""".format(exec_id)
    df = pd.read_sql(sql, conn, index_col="exec_id")
    conn.close()
    if df.shape[0] > 0:
        return {'flow_id': df.loc[exec_id, 'flow_id'], 'prj_nm': df.loc[exec_id, 'prj_nm']}
    else:
        raise Exception(":execution_id: {0} 不存在，请确认是否输入错误".format(exec_id))


def get_executor(host=None, active=None, rstype='exec_id'):
    """
    :param host:  去表executors 查看，都是电脑的名字，所以需要提前配置hosts
    :param active: 0,1 0表示未激活 1 表示激活
    :param rstype: exec_id或者port端口
    :return:{'broker2': 1, 'dps-office': 2} 或者 {'broker2': 12321, 'dps-office': 12321}
    """
    conn = azkaban_mysql_conn()
    if active is None or len(str(active)) > 1:
        active = "where active>=0 "
    else:
        active = "where active={0} ".format(active)
    if host is None:
        host = " "
    else:
        host = " and host='{0}'".format(host)
    sql = "select max(id) exec_id,host,max(port) port from executors {0} {1} group by host".format(active, host)
    # logger.info(sql)
    df = pd.read_sql(sql, conn, index_col="host")
    conn.close()
    return df.to_dict()[rstype]


# def get_project_info_del(info="cron"):
#     """获取项目属性，项目属性配置路径在conf/projects.csv
#     :param 是值获取的属性列，如prj_nm,cron 等
#     :return 返回数据字典 如果有指定flow的flow会将prj_nm和flow_nm拼接
#     """
#     prj = pd.read_csv(os.path.join(conf_path, "project.csv"))
#     prj = prj.fillna("")
#     prj["flow_nm"] = prj['prj_nm'] + prj["flow_nm"]
#     prj = prj.set_index(keys='flow_nm')
#     return prj.to_dict()[info]


def get_project_info(prj_nm, key_nm):
    """
    获取项目的参数配置,例如dw.properties
    :param prj_nm
    :param key_nm 如a.b.c a
    :return
    """
    # prj_path = os.path.join(conf_path, prj_nm)
    # prop_file_sys = os.path.join(prj_path, "system.properties")
    prop_file_prj = get_prj_path(prj_nm, file_type="properties", search_path="conf")  # os.path.join(prj_path, prj_nm + ".properties")
    prop_file = None
    if os.path.exists(prop_file_prj):
        prop_file = prop_file_prj
    if prop_file:
        prop = Properties(prop_file)
        prop.get_properties()  # 获取数值
        return prop.get_value_by_key(key_nm)
    else:
        logger.error(prop_file_prj + "配置文件不存在，请加以配置")
        return None


def get_global_prop(prj_nm=None):
    """
    获取所有参数，生成字典
    :param prj_nm 项目名称
    """
    global_prop = azkaban_config_param  # azkaban 系统参数
    keys = list(global_prop.keys())
    for k in keys:
        if '.' in k:
            global_prop[k.replace(".", "_")] = global_prop[k]
    my_prop = dict(config['base'])
    global_prop.update(my_prop)
    if prj_nm:
        prj_path = os.path.join(conf_path, prj_nm)
        prop_file_prj = os.path.join(prj_path, prj_nm + ".properties")
        if os.path.exists(prop_file_prj):
            prop = Properties(prop_file_prj)
            prop.get_properties()  # 获取数值
            global_prop.update(prop.properties)
    return global_prop


def rplc_cmd_with_prop(cmd, prj_nm, global_prop=None):
    """替换命令里面的参数用来测试"""
    if global_prop is None:
        global_prop = get_global_prop(prj_nm)
    cmd = cmd.replace("$", "")
    match = re.findall(r"{.*?}", cmd)  # r"\{.*?\}"
    for i in match:
        if "." in i:
            cmd = cmd.replace(i, i.replace(".", "_"))
    try:
        cmd = cmd.format(**global_prop)
        if check_file_exists:  # 检查文件是否存在
            tp = cmd.split(" ")
            for i in tp[0:2]:  # 仅检查前两个
                if "." in i:
                    file_nm, file_type = os.path.splitext(i)
                    if file_type in ['.sh', '.py', '.jar']:
                        if i.startswith("/") or i.startswith("\\"):
                            file_nm = i
                        else:
                            file_nm = os.path.join(get_prj_path(prj_nm, file_type="dir", search_path="conf"), i)
                        if not os.path.exists(file_nm):
                            if is_prod:
                                logger.error("项目文件不存在：" + file_nm)
                                return None
                            else:
                                logger.warning("测试环境，项目文件不存在：" + file_nm)
        return cmd
    except Exception as e:
        errors = str(e)
        if 'azkaban_' in errors:
            errors = errors.replace("_", ".")
        logger.warning("使用参数在全局变量中不存在：" + errors)
        return cmd
        # raise Exception("使用参数在全局变量中不存在：" + errors)


def zip_dir(dirpath, filepath):
    """
    压缩指定文件夹
    :param dirpath: 目标文件夹路径
    :param filepath: 压缩文件保存路径+xxxx.zip
    :return: 无
    """
    zips = zipfile.ZipFile(filepath, "w", zipfile.ZIP_DEFLATED)
    for path, dirnames, filenames in os.walk(dirpath):
        # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
        fpath = path.replace(dirpath, '')
        for filename in filenames:
            zips.write(os.path.join(path, filename), os.path.join(fpath, filename))
    zips.close()


def crt_sys_prop(prj_nm):
    """生成系统参数文件system.properties"""
    path = get_prj_path(prj_nm, search_path="temp")
    with open(path, 'w') as f:
        for i in config['base'].items():
            if not ('pwd' in i[0] or i[0] in config.defaults().keys()):
                f.write("{0} = {1}\n".format(i[0], i[1]))
        f.write("main_path={0}\n".format(main_path))
        f.write("etl_dt=${azkaban.flow.start.year}-${azkaban.flow.start.month}-${azkaban.flow.start.day}\n")
        f.write("etl_dtm=${azkaban.flow.start.year}-${azkaban.flow.start.month}-${azkaban.flow.start.day} ")
        f.write("${azkaban.flow.start.hour}:${azkaban.flow.start.minute}:${azkaban.flow.start.second}\n")
        f.write("etl_tm=${azkaban.flow.start.hour}:${azkaban.flow.start.minute}:${azkaban.flow.start.second}\n")
        f.write("batch_dt=${azkaban.flow.start.year}-${azkaban.flow.start.month}-${azkaban.flow.start.day}\n")
        f.write("batch_id=${azkaban.flow.start.year}${azkaban.flow.start.month}${azkaban.flow.start.day}")
        f.write("${azkaban.flow.start.hour}${azkaban.flow.start.minute}${azkaban.flow.start.second}\n")


def check_depend_if_in_flow(strs, lists):
    """
    检查依赖
    :param strs:
    :param lists:
    :return:
    """
    if len(strs) > 1:
        tp = strs.split(",")
        for i in tp:
            if i not in lists:
                raise Exception("被依赖表{0}不在该工作流中，请注意添加。不然无法完成".format(i))


def crt_job_file(prj_nm):
    """
    创建job文件
    :param prj_nm:
    :return:
    """
    prj_conf_path = os.path.join(conf_path, prj_nm)
    filepath = os.path.join(prj_conf_path, prj_nm + '.csv')
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        prj_path = os.path.join(temp_path, prj_nm)
        df['command'] = df['command'].fillna(
            "echo 'Exec ${azkaban.job.id} of ${azkaban.flow.flowid} with batch_id:${batch_id} etl_dt:${etl_dt} batch_dt:${batch_dt}'")
        if os.path.exists(prj_path):
            import shutil
            shutil.rmtree(prj_path)  # 清空项目对应的临时目录，一般是temp目录下
        os.makedirs(prj_path)
        df = df.fillna('')
        df['job_nm'] = df['job_nm'].apply(lambda x: x.strip())
        jobs = set(df['job_nm'])  # 需要配置的job set
        df['dependencies'] = df['dependencies'].apply(lambda x: x.strip().replace("，", ",").replace(" ", ""))
        df['dependencies'].apply(lambda x: check_depend_if_in_flow(x, jobs))  # 检查依赖job是否在配置中
        for i in df.to_dict(orient='records'):
            job_path = os.path.join(prj_path, i['job_nm'] + '.job')
            with open(job_path, 'w') as job:
                job.write("type = command\n")
                if len(i['dependencies']) > 1:
                    job.write("dependencies = " + i['dependencies'].strip().replace("，", ",").replace(" ", "") + "\n")
                job.write("retries = {0}\n".format(get_project_info(prj_nm, "retries") or 3))
                job.write("retry.backoff = {0}\n".format(get_project_info(prj_nm, "retry.backoff") or 60000))  # 重试的间隔（毫秒）
                if get_project_info(prj_nm, "failure.emails"):
                    # 失败邮件通知 如果项目配置文件有配置
                    job.write("failure.emails = {0}\n".format(get_project_info(prj_nm, "failure.emails")))
                if get_project_info(prj_nm, "success.emails"):
                    # 失败邮件通知 如果项目配置文件有配置
                    job.write("success.emails = {0}\n".format(get_project_info(prj_nm, "success.emails")))
                if get_project_info(prj_nm, "working.dir"):
                    # 失败邮件通知 如果项目配置文件有配置
                    job.write("working.dir = {0}\n".format(get_project_info(prj_nm, "working.dir")))
                # command = "command = {0} {1} {2} {3}".format(i['command'].strip(), i['arg1'], i['arg2'], i['arg3'])
                command = "command = {0}".format(i['command'].strip())
                check_cmd = rplc_cmd_with_prop(i['command'].strip(), prj_nm)
                if check_cmd:
                    job.write(command.strip())
                else:
                    logger.error("{0}项目job文件生中断，命令解析不通过".format(prj_nm))
                    raise Exception("{0}项目job文件生中断，命令解析不通过".format(prj_nm))
        # 生成end_flow
        depend_job_set = set()  # 依赖的job set 集

        def add_depend_job_set(strs):
            """计算被依赖的job"""
            if strs:
                tp = strs.split(",")
                depend_job_set.update(tp)  # 更新 job set

        df[df['dependencies'].notna()]['dependencies'].apply(add_depend_job_set)
        not_depended_jobs = ','.join(jobs - depend_job_set)  # 算出不被依赖的job
        job_path = os.path.join(prj_path,  prj_nm + "_end_flow.job")
        with open(job_path, 'w') as job:  # 生成end_flow_prj_nm.job 文件
            job.write("type = command\n")
            job.write("dependencies = " + not_depended_jobs + "\n")
            job.write("command = echo 'all flow end' \n")
        # end_flow job生成完成
        crt_sys_prop(prj_nm)  # 生成全局系统参数文件  system.properies
        copy_local_param(prj_nm)  # 把项目参数（例如dw.properies）输出到全局系统参数文件
        # copy_dir(prj_conf_path, os.path.join(prj_path, 'scripts'), ignore_file_type=['job', 'flow', 'project'])
        # copy_dir(prj_conf_path, prj_path, only_file_type=['job', 'flow', 'project'])
        copy_dir(prj_conf_path, prj_path, ignore_file_type=['properties'])
        zip_path = prj_path + '.zip'
        zip_dir(prj_path, zip_path)   # 将目标目录压缩成zip文件
        # logger.info("{0}项目压缩完成，文件路径是：{1}".format(prj_nm, zip_path))
        return zip_path  # 返回zip文件路径
    else:
        logger.error("文件不存在:" + filepath)
        return None


def check_cron(cron):
    """
    检查cron时间格式是否合规
    :param cron:
    :return:
    """
    if cron and " " in cron:
        try:
            pt = Cron(cron)
            logger.info("cron格式校验通过,下次执行时间: " + str(pt.get_next()))
            return True
        except Exception as e:
            logger.error(str(e))
            logger.error("cron不能为空或者格式不对,你输入的是{0}".format(cron))
            return False
    else:
        logger.warning("cron不能为空或者格式不对,你输入的是{0}".format(cron))
        return False


def get_prj_path(prj_nm, file_type="properties", search_path="conf"):
    """
    :param prj_nm:
    :param file_type:
    :param search_path:
    :return:
    """
    if search_path == "conf":
        search_path = os.path.join(conf_path, prj_nm)
        if file_type in ["csv", "properties"]:
            return os.path.join(search_path, prj_nm + "." + file_type)
        else:
            return search_path
    else:
        search_path = os.path.join(temp_path, prj_nm)
        if file_type == "properties":
            return os.path.join(search_path, "system.properties")
        else:
            return search_path


def copy_local_param(prj_nm):
    """读取项目下的配置文件内容，并输出到正式项目system.properties"""
    rd_file = get_prj_path(prj_nm, file_type="properties", search_path="conf")
    # os.path.join(os.path.join(conf_path, prj_nm), prj_nm + ".properties")
    wt_file = get_prj_path(prj_nm, file_type="properties", search_path="temp")
    # os.path.join(os.path.join(temp_path, prj_nm), "system.properties")
    if not os.path.exists(rd_file):
        logger.error(rd_file + "文件不存在，请创建")
        return
    if not os.path.exists(wt_file):
        logger.error(wt_file + "参数文件没有生成")
        return
    with open(wt_file, 'a') as w:
        with open(rd_file, 'r') as r:
            line = r.readline()
            while line:
                if "=" in line.strip():
                    tp = line.strip().split("=")[0].strip()
                    if tp in job_config_keys or 'cron' in tp:
                        # 特殊参数 和job参数不输入到systemp.properties
                        pass
                    else:
                        w.write(line)
                line = r.readline()


def extract_json(response):
    """
    解析 JSON 来自  response.
    :param response: Request response object.

    """
    try:
        json = response.json()
    except ValueError as err:  # this should never happen
        logger.error('没有json格式体 :\n%s', str(response.text, 'utf-8'))
        raise err
    else:
        if 'error' in json:
            raise json['error']
        elif json.get('status') == 'error':
            raise json['message']
        else:
            return json


def check_failed_job():
    """
    检查失败的job
    :return:
    """
    sql = """
    select
        distinct
        t1.name prj_nm,
        t2.flow_id,
        t2.job_id,
        input_params, 
        t2.exec_id,l.log logs,
        FROM_UNIXTIME(t2.start_time/1000, '%Y-%m-%d %H:%i:%s') start_time
        from execution_jobs t2 left join projects t1 on t2.project_id=t1.id 
        left join execution_logs l on t2.exec_id=l.exec_id and t2.job_id=l.name and t2.attempt=l.attempt
        where  t2.status=70 and unix_timestamp()-(t2.start_time/1000)<=3600 and t2.attempt=0
    """
    conn = azkaban_mysql_conn()
    err_df = pd.read_sql(sql, conn)
    if err_df.shape[0] > 0:
        for i in err_df.index:
            input_params = azkaban_blob_to_str(err_df.loc[i, 'input_params'])
            print(azkaban_blob_to_str(err_df.loc[i, 'logs']))


def check_file_depend():
    """检查单个文件里面的依赖 读取文件代码，找到依赖表"""
    pass


#
# if __name__ == "__main__":
#     print(get_projects())
#
#     logger = logging.getLogger()
#     logger.info('debug message’')
#     # logger.info('info message’')
#     # logger.warning('warn message’')
#     # logger.error('error message’')

"""
select t.name, t.last_modified_by, cast(t.last_failed_exec_id as char(7)) as last_failed_exec_id, 
                      cast(t.last_1hour_failed_times as char(4)) as last_1hour_failed_times, 
                      cast(t.last_1hour_run_times as char(4)) as last_1hour_run_times 
                from (select t1.name, t1.last_modified_by, 
                             max((case when t2.status=70 then exec_id else 0 end)) as last_failed_exec_id, 
                             count(distinct (case when t2.status=70 then t2.exec_id else null end)) as last_1hour_failed_times, 
                             count(distinct t2.exec_id) as last_1hour_run_times 
                        from execution_jobs t2 left join projects t1 on t2.project_id=t1.id 
                      -- where unix_timestamp()-(t2.start_time/1000)<=3600 
                       group by t1.name, t1.last_modified_by 
                      -- having count(distinct (case when t2.status=70 then t2.exec_id else null end))>0
                      ) t
                      
                      
                      select
                            distinct
                       t1.name, t1.last_modified_by,t2.flow_id,t2.job_id,input_params, t2.exec_id,l.log,
                       FROM_UNIXTIME(t2.start_time/1000, '%Y-%m-%d %H:%i:%s') start_time
                       from execution_jobs t2 left join projects t1 on t2.project_id=t1.id 
                          left join execution_logs l on t2.exec_id=l.exec_id and t2.job_id=l.name and t2.attempt=l.attempt
                       where  t2.status=70 and unix_timestamp()-(t2.start_time/1000)<=3600 and t2.attempt=0
"""
