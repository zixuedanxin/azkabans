from requests import HTTPError
from azkaban.utils import *
from azkaban.config import logger, login_pwd, login_user, azkaban_url
import requests


# 登录azkaban 获取相关信息
class Cookies:
    def __init__(self, user=login_user, pwd=login_pwd):
        self.login_data = {'action': 'login', 'username': user, 'password': pwd}
        try:
            resp = requests.post("{azkaban_url}".format(azkaban_url=azkaban_url), data=self.login_data)
            self.cookies = resp.cookies
            self.session_id = resp.json()['session.id']
            logger.debug("azkaban登录成功")
        except Exception as e:
            logger.error("登录错误：" + str(e))
            # return None  # 对于重启就不需要登录成功

    def get_cookies(self):
        """
        获取cookies
        :return:
        """
        return self.cookies

    def get_session_id(self):
        """
        获取session_id
        :return:
        """
        return self.session_id

    def refresh(self):
        """刷新登录"""
        try:
            resp = requests.post("{azkaban_url}".format(azkaban_url=azkaban_url), data=self.login_data)
        except Exception as e:
            raise Exception(str(e))
        self.cookies = resp.cookies
        return self.cookies

    @staticmethod
    def fetch_projects():
        """获取所有的项目名称
        """
        # response = requests.get(
        #     azkaban_url + '/index?all',
        #     cookies=self.cookies
        # )
        # from bs4 import BeautifulSoup
        # # logger.info("Response: \n%s", response.content)
        # soup = BeautifulSoup(response.text, "html.parser")
        # ul = soup.find_all('ul', {'id': 'project-list'})
        # prj_list = []
        # for i in ul:
        #     divs = i.find_all('a')
        #     for j in divs:
        #         prj_list.append(j.text)
        # logger.info(prj_list)
        return get_projects()

    def _request(self, method, endpoint, include_session='cookies', **kwargs):
        """
        通过sessionc创建request请求
        :param method: HTTP
        :param endpoint: executor 或 schedule azkaban链接参数
        :param include_session: Where to include the `session_id` (possible values:`'cookies'`, `'params'`, `False`).
        :param kwargs: Keyword arguments passed to :func:`_azkaban_request`.
        """
        full_url = '%s/%s' % (azkaban_url, endpoint.lstrip('/'))

        if not self.session_id:
            logger.debug('session_id过期.')
            self.refresh()

        def _send_request():
            """发送请求"""
            if include_session == 'cookies':
                kwargs.setdefault('cookies', {})['azkaban.browser.session.id'] = self.session_id
            elif include_session == 'params':
                kwargs.setdefault('data', {})['session.id'] = self.session_id
            elif include_session:
                raise ValueError('Invalid `include_session`: %r' % (include_session,))
            return azkaban_request(method, full_url, **kwargs)

        response = _send_request()
        # if not self.is_valid(response):
        #     self._refresh()
        #     response = _send_request()

        # `_refresh` raises an exception rather than letting an unauthorized second
        # request happen. this means that something is wrong with the server.
        # if not self.is_valid(response):
        #     raise AzkabanError('Azkaban server is unavailable.')

        try:
            response.raise_for_status()
        except HTTPError as err:  # catch, log, and reraise
            logger.warning(
                '获取无效的response  %s:\n%s',
                response.request.url, response.content
            )
            raise err
        else:
            return response

    def get_execution_status(self, exec_id):
        """
        获取执行状态
        :param exec_id: Execution ID
        """
        logger.debug('Fetching status for execution %s.', exec_id)
        return extract_json(self._request(
            method='GET',
            endpoint='executor',
            params={
                'execid': exec_id,
                'ajax': 'fetchexecflow',
            },
        ))

    def get_execution_logs(self, exec_id, offset=0, limit=50000):
        """
        获得执行日志.
        :param exec_id: Execution ID.
        :param offset: Log offset 开始位置.
        :param limit: 下载的大小限制.

        """
        logger.debug('Fetching logs for execution %s.', exec_id)
        return extract_json(self._request(
            method='GET',
            endpoint='executor',
            params={
                'execid': exec_id,
                'ajax': 'fetchExecFlowLogs',
                'offset': offset,
                'length': limit,
            },
        ))

    def cancel_execution(self, exec_id):
        """
        取消执行execution.
        :param exec_id: Execution ID.

        """
        logger.debug('Cancelling execution %s.', exec_id)
        res = extract_json(self._request(
            method='GET',
            endpoint='executor',
            params={
                'execid': exec_id,
                'ajax': 'cancelFlow',
            },
        ))
        if 'error' in res:
            raise Exception('Execution %s is not running.', exec_id)
        else:
            logger.info('Execution %s cancelled.', exec_id)
        return

    def get_sla(self, schedule_id):
        """
         获取SLA设置信息. 基本不用
        :param schedule_id: Schedule Id - obtainable from get_schedule

        """
        logger.debug('Retrieving SLA for schedule ID %s.', schedule_id)
        res = extract_json(self._request(
            method='GET',
            endpoint='schedule',
            params={
                'ajax': 'slaInfo',
                'scheduleId': schedule_id
            },
        ))
        logger.info('Retrieved SLA for schedule ID %s.', schedule_id)
        if 'settings' not in res:
            raise Exception('Failed to get SLA; check that an SLA exists.')
        return res

    def set_sla(self, schedule_id, email, settings):
        """
        为SLA设置执行计划 基本不用
        :param schedule_id: Schedule ID.
        :param email: Array of emails to receive notifications.
        :param settings: Array of comma delimited strings of SLA settings
          consisting of:
          + job name - blank for full workflow
          + rule - SUCCESS or FINISH
          + duration - specified in hh:mm
          + email action - bool
          + kill action - bool

        """
        logger.debug('Setting SLA for schedule Id %s.', schedule_id)
        request_data = {
            'ajax': 'setSla',
            'scheduleId': schedule_id,
            'slaEmails': ','.join(email),
        }
        for i, setting in enumerate(settings):
            request_data['settings[%s]' % (i,)] = setting
        res = extract_json(self._request(
            method='POST',
            endpoint='schedule',
            data=request_data,
        ))
        logger.info('Set SLAs for schedule Id %s.', schedule_id)
        return res
