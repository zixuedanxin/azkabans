from azkaban.config import logger
import os
import re


# main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# config_file = os.path.join(os.path.join(main_path, "conf"), "config.ini")
# logging.config.fileConfig(config_file)
# logger = logging.getLogger()


class Properties(object):

    def __init__(self, file_name):
        self.fileName = file_name
        self.properties = {}

    def __get_dict(self, str_name, dict_name, value):

        if str_name.find('.') > 0:
            k = str_name.split('.')[0]
            dict_name.setdefault(k, {})
            return self.__get_dict(str_name[len(k) + 1:], dict_name[k], value)
        else:
            dict_name[str_name] = value
            return

    def get_properties(self):
        try:
            pro_file = open(self.fileName, 'Ur')
            for line in pro_file.readlines():
                line = line.strip().replace('\n', '')
                if line.find("#") != -1:
                    line = line[0:line.find('#')]
                if line.find('=') > 0:
                    strs = line.split('=')
                    strs[1] = line[len(strs[0]) + 1:]
                    self.__get_dict(strs[0].strip(), self.properties, strs[1].strip())
        except Exception as e:
            logger.info("本地的本质文件不规范，注意出现了com key值 就不能出现 com.hy的key值")
            raise Exception(e)
        else:
            pro_file.close()
        return self.properties

    def get_value_by_key(self, str_name, jsdata=None):
        if jsdata is None:
            jsdata = self.get_properties()
        if str_name.find('.') > 0:
            k = str_name.split('.')[0]
            str_name = str_name.replace(k + ".", "")
            # print(str_name, jsdata.get(k, {}))
            tp_js = jsdata.get(k, {})
            if type(tp_js) == dict:
                return self.get_value_by_key(str_name=str_name, jsdata=jsdata.get(k, None))
        else:
            return jsdata.get(str_name, None)

    def put(self, key, value):
        self.properties[key] = value
        self.replace_property(self.fileName, key + '=.*', key + '=' + value, True)

    @staticmethod
    def replace_property(file_name, from_regex, to_str, append_on_not_exists=True):
        import tempfile
        tmpfile = tempfile.TemporaryFile(mode='w+')
        if os.path.exists(file_name):
            r_open = open(file_name, 'r')
            pattern = re.compile(r'' + from_regex)
            found = None
            for line in r_open:
                if pattern.search(line) and not line.strip().startswith('#'):
                    found = True
                    line = re.sub(from_regex, to_str, line)
                tmpfile.write(line)
            if not found and append_on_not_exists:
                tmpfile.write('\n' + to_str)
            r_open.close()
            tmpfile.seek(0)
            content = tmpfile.read()
            if os.path.exists(file_name):
                os.remove(file_name)
            w_open = open(file_name, 'w')
            w_open.write(content)
            w_open.close()
            tmpfile.close()
        else:
            logger.error("文件 %s not found" % file_name)

    def to_dict(self, json=None, start=None):
        """
        返回字符串映射的字典格式
        :param json:
        :param start:
        :return:
        """
        if json is None:
            json = self.properties
        init_dict = {}
        for i in json:
            tp = json[i]
            dic_deep = {}
            if start is None:
                new_key = i
            else:
                new_key = start + "." + i
            if isinstance(tp, dict):
                dic_deep = self.to_dict(tp, start=new_key)
            else:
                init_dict[new_key] = tp
            if dic_deep:
                init_dict.update(dic_deep)
        return init_dict


# if __name__ == '__main__':
#     pro = Properties('/home/xzh/dps/etl/azkabans/conf/dw/dw.properties')
#     pro.get_properties()
#     print(pro.properties)
#     print(pro.properties.keys())
#
#     print(pro.to_dict())
#     # pro.put("sdshuake",'sggfhee44')
#     # print(pro.get_value_by_key("end"))
