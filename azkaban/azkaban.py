# import configparser
# import requests
import json
# import logging
# import os
from .utils import *
from .Cookies import Cookies
from .config import logger, check_interval, backup_path

# logger = logging.getLogger()
cookies = Cookies()


class Project:
    def __init__(self, project, description=None, cookies_fetcher=cookies):
        self.name = project
        self.description = description
        if description is None:
            self.description = project
        self.cookies_fetcher = cookies_fetcher
        if project in get_projects():
            self.crt_flag = True
        else:
            self.crt_flag = False

    def create_prj(self):
        """创建项目，先看看有木有存在"""
        if self.crt_flag:
            logger.info("项目已经存在不能创建")
            return self
        create_data = {
            'name': self.name,
            'description': self.description
        }
        resp = requests.post("{azkaban_url}/manager?action=create".format(azkaban_url=azkaban_url), data=create_data,
                             cookies=self.cookies_fetcher.get_cookies())
        if resp.status_code != 200:
            raise Exception('Error happened when creating project {project} to azkaban'.format(project=self.name))
        logger.info(resp.content)
        logger.info('project {project} creatd : {status}'.format(project=self.name,
                                                                 status=json.loads(str(resp.content, 'utf-8'))['status']))
        return self

    def del_prj(self):
        """删除项目"""
        if len(self.fetch_flow_schedule()) > 0:
            logger.info("该项目有执行计划，不能删除")
        if self.crt_flag and self.download_zip():
            resp = requests.get("{azkaban_url}/manager?delete=true&project={name}".format(azkaban_url=azkaban_url, name=self.name),
                                cookies=self.cookies_fetcher.get_cookies())
            if resp.status_code != 200:
                raise Exception('Error happened when delete project {project} to azkaban'.format(project=self.name))
            logger.info('删除Project:' + self.name)
            return self

    def download_zip(self):
        """下载zip文件"""
        if self.crt_flag:
            url = "{azkaban_url}/manager?session.id={id}&project={project}&download=True".format(id=self.cookies_fetcher.get_session_id(),
                                                                                                 azkaban_url=azkaban_url,
                                                                                                 project=self.name)
            # headers = {
            #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit \
            #            /537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
            #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image \
            #            /webp,image/apng,*/*;q=0.8',
            #     'Accept-Encoding': 'gzip, deflate',
            #     'Accept-Language': 'zh-CN, zh; q=0.9',
            #     'Referer': url,
            #     'Upgrade-Insecure-Requests': '1',
            #     'azkaban_url': azkaban_url,
            # }
            resp = requests.get(url,  # headers=headers,
                                stream=True)  # cookies=self.cookies_fetcher.get_cookies(),
            now_time = get_current_timekey()
            backup_dt_dir = os.path.join(backup_path, now_time[0:8])
            if not (os.path.exists(backup_dt_dir)):
                os.makedirs(backup_dt_dir)
            file_path = os.path.join(backup_dt_dir, self.name + "_" + now_time + '.zip')
            with open(file_path, "wb") as code:
                code.write(resp.content)
            if resp.status_code != 200:
                raise Exception('下载{project}项目文件失败'.format(project=self.name))
            logger.info("下载ZIP文件完成" + self.name + ": " + file_path)
            return True
        else:
            logger.info("项目没有文件无需备份")
            return True

    def upload_zip(self, zip_file):
        """上传zip文件"""
        if self.download_zip():
            # 备份文件成功
            logger.info("备份{0}项目文件成功".format(self.name))
            files = {'file': (os.path.basename(zip_file), open(zip_file, 'rb'), 'application/zip')}
            upload_data = {
                'project': self.name,
                'ajax': 'upload',
            }
            resp = requests.post("{azkaban_url}/manager".format(azkaban_url=azkaban_url), data=upload_data,
                                 cookies=self.cookies_fetcher.get_cookies(),
                                 files=files)
            if resp.status_code != 200:
                logger.error(str(resp.content, 'utf-8'))
                raise Exception('上传ZIP文件失败：{name} '.format(name=zip_file))
            logger.info("上传ZIP文件完成：" + self.name)
        else:
            raise Exception('项目文件{name}：文件备份失败'.format(name=self.name))

    def fetch_flow(self):
        """获取所有工作流"""
        flows_resp = requests.get(
            '{azkaban_url}/manager?ajax=fetchprojectflows&project={project}'.format(azkaban_url=azkaban_url, project=self.name),
            cookies=self.cookies_fetcher.get_cookies())
        if flows_resp.status_code != 200:
            logger.error(str(flows_resp.content, 'utf-8'))
            raise Exception('Error happened when fetch flow from {project} in azkaban'.format(project=self.name))
        flows = json.loads(str(flows_resp.content, 'utf-8'))['flows']
        tp = []
        for flow in flows:
            tp.append(flow['flowId'])
        #     yield Flow(self.name, flow['flowId'], cookies_fetcher=self.cookies_fetcher)
        return tp

    def schedule_flows(self, cron=None, flows=None):
        """将所有的工作流列入执行计划"""
        all_flows = self.fetch_flow()
        if cron is None:
            # try:
            cron = get_project_info(prj_nm=self.name, key_nm="cron." + self.name)  # prj_cron.get(self.name, None)
            if cron is None and get_project_info(prj_nm=self.name, key_nm="cron") is None:
                logger.error(self.name + "的 system.properties中没有配置定时器cron请配置")
                return
        if flows is None:
            flows = all_flows
        for f in all_flows:
            if f in flows:
                logger.info("设置项目{0}的工作流{1}执行计划".format(self.name, f))
                flow = Flow(self.name, f, self.cookies_fetcher)
                flow.schedule(cron)

    def fetch_flow_schedule(self):
        """将所有的工作流列入执行计划"""
        all_flows = self.fetch_flow()
        schd_id = []
        for f in all_flows:
            flow = Flow(self.name, f, self.cookies_fetcher)
            sid = flow.fetch_schedule()
            if sid:
                schd_id.append(sid)
        return schd_id


class Flow:
    """工作流类"""

    def __init__(self, prj_name, flow_id, disabled=None, flow_override=None, cookies_fetcher=cookies):
        self.prj_name = prj_name
        self.cookies_fetcher = cookies_fetcher
        self.flowId = flow_id
        if disabled:
            self.disabled = "&disabled=" + str(disabled).replace("'", "\"")
        else:
            self.disabled = ""
        if flow_override and type(flow_override) == dict:
            tp = []
            for k in flow_override.keys():
                tp.append("flowOverride[{0}]={1}".format(k, flow_override[k]))
            self.flow_override = '&' + '&'.join(tp)
        else:
            self.flow_override = ""

    def execute(self):
        """
        执行工作流
        :return: 返回执行id
        """
        logger.info('开始执行flow {flow}'.format(flow=self.flowId))
        url = '{azkaban_url}/executor?ajax=executeFlow&project={project}&flow={flow}' + self.disabled + self.flow_override
        url = url.format(
            azkaban_url=azkaban_url,
            project=self.prj_name,
            flow=self.flowId)
        # logger.info("执行url：" + url)
        flows_resp = requests.get(
            url,
            cookies=self.cookies_fetcher.get_cookies()
        )
        rs = str(flows_resp.content, 'utf-8')
        if flows_resp.status_code != 200 or 'error' in rs:
            logger.error(rs)
            raise Exception('执行{flow} 报错'.format(flow=self.flowId))
        else:
            # logger.info(rs)
            exec_id = json.loads(rs)['execid']
            logger.info(('开始执行{flow}，execid是{exec_id}'.format(flow=self.flowId, exec_id=exec_id)))
            return FlowExecution(self.prj_name, self.flowId, exec_id, self.cookies_fetcher)

    def fetch_job(self):
        """获取工作流的job"""
        flows_resp = requests.get(
            '{azkaban_url}/manager?ajax=fetchflowgraph&project={project}&flow={flow}'.format(azkaban_url=azkaban_url, project=self.prj_name,
                                                                                             flow=self.flowId),
            cookies=self.cookies_fetcher.get_cookies())
        if flows_resp.status_code != 200:
            raise Exception('Error happened when fetch job from {0} in {1}'.format(self.flowId, self.prj_name))
        jobs = json.loads(str(flows_resp.content, 'utf-8'))['nodes']
        logger.info(jobs)
        return jobs

    def schedule(self, cron=None):
        """
        安排执行计划
        """
        flow_cron = get_project_info(self.prj_name, "cron." + str(self.flowId).strip())
        if flow_cron:
            cron = flow_cron  # prj_cron.get(self.prj_name + self.flowId, None)
        if check_cron(cron):
            data = {
                'session.id': self.cookies_fetcher.get_session_id(),
                'ajax': u'scheduleCronFlow',
                'projectName': self.prj_name,
                'flow': self.flowId,
                'cronExpression': cron
            }

            response = requests.post(
                azkaban_url + '/schedule',
                data=data
            )
            rs = str(response.content, 'utf-8')
            if response.status_code != 200 or 'error' in rs:
                logger.info("秒 分 时 日 月 周 data: \n%s", data)
                logger.error(rs)
                logger.error("{0}设置执行计划失败".format(self.prj_name))
                return False
            else:
                logger.info("{0} flow:{1}设置执行计划成功".format(self.prj_name, self.flowId))
                return True
        else:
            # logger.error("{0}设置执行计划失败,请设置正确的cron时间格式.ERROR for:{1}".format(self.prj_name, cron))
            return False

    def fetch_schedule(self):
        """
        获取执行计划
        """
        prj_id = get_projects().get(self.prj_name)
        response = requests.get(
            azkaban_url + '/schedule',
            params={
                'session.id': self.cookies_fetcher.get_session_id(),
                'ajax': 'fetchSchedule',
                'projectId': prj_id,
                'flowId': self.flowId
            }
        )
        if response.status_code != 200:
            logger.info(str(response.content, 'utf-8'))
            raise Exception("{0}获取执行计划列表失败".format(self.prj_name))
        else:
            # logger.debug(str(response.content, 'utf-8'))
            try:
                schd_id = response.json()['schedule']['scheduleId']
                logger.debug("{0} flow:{1}获取执行计划ID是{2}".format(self.prj_name, self.flowId, schd_id))
                return schd_id
            except Exception as e:
                logger.debug(str(e))
                logger.info("{0} flow:{1} 没有设置执行计划".format(self.prj_name, self.flowId))
                return None

    def unscheduled(self):
        """
        取消执行计划
        """
        schd_id = self.fetch_schedule()
        if schd_id:
            data = {
                u'session.id': self.cookies_fetcher.get_session_id(),
                u'action': u'removeSched',
                u'scheduleId': schd_id
            }
            response = requests.post(
                azkaban_url + '/schedule',
                data=data
            )
            if response.status_code != 200:
                logger.info("Request data: \n%s", data)
                logger.error(str(response.content, 'utf-8'))
                raise Exception("{0} 取消执行计划失败".format(self.prj_name))
            else:
                logger.info("{0} flow:{1} 执行计划id={2}已经被取消".format(self.prj_name, self.flowId, schd_id))
                return True


class FlowExecution:
    job_status_dict = dict()
    flow_timeout = 180

    def __init__(self, prj_name, flow_id, exec_id, cookies_fetcher=cookies):
        self.prj_name = prj_name
        self.flowId = flow_id
        self.exec_id = exec_id
        self.cookies_fetcher = cookies_fetcher

    def resume_flow(self):
        """恢复执行"""
        target = '%s/executor?ajax=executeFlow&project=%s&flow=%s&disabled=%s' % (
            azkaban_url, self.prj_name.name, self.flowId, get_str_set(self.job_status_dict.get('SUCCEEDED')))
        resp = requests.get(target, cookies=self.cookies_fetcher.get_cookies())
        contents = resp.content
        new_exec_id = json.loads(contents)['execid']
        logger.info('old exec_id {old} to new one {new}'.format(old=self.exec_id, new=new_exec_id))
        self.exec_id = new_exec_id

    def handle_timeout(self):
        """
        设置超时后先杀死进程然后恢复执行,循环监控
        :return:
        """
        while True:
            logger.info('checking to execute flow {flow}, {exec_id}'.format(flow=self.flowId, exec_id=self.exec_id))
            result = self.get_flow_exec_info()
            self.refresh_flow_execution()
            start_time = result['startTime']
            start_time /= 1000
            if result['status'] == 'KILLED':
                logger.info("{execid} has been killed.".format(execid=self.exec_id))
                break
            elif result['status'] == 'SUCCEEDED':
                logger.info("{execid} has been SUCCEEDED.".format(execid=self.exec_id))
                break
            elif result['status'] == 'FAILED':
                logger.info("{execid} has been FAILED.".format(execid=self.exec_id))
                break
            else:
                if start_time > 0 and int(time.time()) - start_time > 60 * self.flow_timeout \
                        and result['endTime'] == -1:
                    logger.info('reached timeout threshold \n')
                    self.cancel()
                    time.sleep(60)
                    self.resume_flow()
            time.sleep(check_interval)

    def refresh_flow_execution(self):
        """
        刷新执行ID的状态.
        :return:
        """
        result = self.get_flow_exec_info()
        for dd in result['nodes']:
            cu = self.job_status_dict.get(dd['status'], set())
            cu.add(dd['id'])
            self.job_status_dict[dd['status']] = cu
        for k, v in self.job_status_dict.items():
            logger.info('%s  status: %s : %d/%d \n' % (get_current_timekey(), k, len(v), len(result['nodes'])))

    def get_flow_exec_info(self):
        """
        获取执行信息
        :return:
        """
        target = '%s/executor?ajax=fetchexecflow&execid=%s' % (azkaban_url, self.exec_id)
        resp = requests.get(target, cookies=self.cookies_fetcher.get_cookies())
        return json.loads(str(resp.content, 'utf-8'))

    def cancel(self):
        """取消执行"""
        target = '%s/executor?ajax=cancelFlow&execid=%s' % (azkaban_url, self.exec_id)
        resp = requests.get(target, cookies=self.cookies_fetcher.get_cookies())
        if resp.status_code != 200:
            logger.info(str(resp.content, 'utf-8'))

# if __name__ == '__main__':
#     prj = Project('webtest2', 'test1 web ')
#     # prj.download_zip()
#     # prj.create_prj()
#     prj.upload_zip('/home/xzh/OneDrive/myazkaban_job_test/test1ok.zip')  # sample_test2
#     prj.fetch_flow()
#     # for i in prj.fetch_flow():
#     #     i.execute()
#     fl = Flow('webtest2', '4', disabled=["1"], flow_override={'props6': 'over6', 'props5': 'over5'})
#     # fl.schedule('/5 0,15,30,45 * ? * *')
#     # fl.fetch_schedule()
#     # fl.unscheduled()
#     fl.execute()
#     # print(logger.name)
#     # his=FlowExecution('webtest2','4','18')
#     # print(his.get_flow_exec_info())
