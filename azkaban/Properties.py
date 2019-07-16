from .config import logger

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

#
# if __name__ == '__main__':
#     pro = Properties('/home/xzh/mypython/etlpy/azkaban/azkabans/conf/dw/dw.properties')
#     pro.get_properties()
#     print(pro.properties.keys())
#     print(pro.get_value_by_key("end"))
