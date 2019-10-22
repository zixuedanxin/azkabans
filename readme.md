
# Azkabans介绍

    本项目主要用于azkaban远程发布项目，定时发布项目等辅助功能。
    1、所有的作业都是shell命令执行，这样使得作业配置更简单
    2、减少job文件的配置，所有job会根据配置文件自动生成，校验依赖作业，并自动打包上传,自动加入定时任务
    3、把azkaban当中任务执行和查看的介质，任务配置以批量配置的方式，减少工作量、方便管理项目
    4、azkaban的项目发布需把整个项目都重新发布，这个比较麻烦还容易出错。借助azkabans更容易管理
    
  


# **azkaban目录：**

    1、登录操作（cookies.py）、项目操作azkaban.py
    
    2、cron.py是对时间的解析
    
    3、propertie是对参数文件读取
    
    4、config.py对conf目录下的config.ini文件读取
    
    5、utils,py常用函数库


# **backup目录**

    用于备份项目的历史zip文件。
    
    上传新的zip文件之前先把老的版本下载下来本分到此文件夹下
    
    也可以单独指定备份目录. eg: config.ini backup_path=/home/xzh/azkaban/backup

# **conf目录**

1、全局配置文件config.ini

    [base]项是必须项,这里的配置项也会作为全局变量输出到system.properties文件中
            #  定义base分组，必须配置
            [base]
            azkaban_url=http://broker2:8081  # 你的azkaban登录的url azkaban有的限制域名访问，需要设置或改成IP
            login_user=testAdmin             # azkaban 登录用户名
            login_pwd=testAdmin              # azkaban 登录密码
            check_interval=600               # 休息间隔时间,任务执行失败从新尝试等待时间 循环监控时间
            # mysql配置
            mysql_host=broker2
            mysql_port=3306
            mysql_user=xxxx
            mysql_pwd=xxxx
            mysql_db=azkaban
            # ETL文件目录
            etl_path=/home/xzh/dps/etl/
            
            
2、单个项目配置，如dw是项目名为dw的配置文件夹

	2.1、dw.csv主要配置项目job依赖和job 命令
	
	以下是个csv文件文件, （job_nm是个job名称;dependencies是指项目依赖job,多个以逗号或者空格分开 ;command 是该job的执行命令）

	job_nm      , dependencies      ,   command (表头)
    start       ,                   ,
    el_crm      ,   start           ,   sh scripts/testa.sh
    el_hyf      ,   start           ,   python -V
    dim_city    ,   "el_crm,el_hyf" ,   sh scripts/testb.sh ${azkaban.job.id} ${firstName}
    dim_mon     ,   dim_city        ,   echo ${etl_dtm}
    mytest      ,   dim_mon         ,   sh scripts/testb.sh ${azkaban.job.id} ${firstName}
    end         ,   mytest          ,
    
    解释：dim_city 依赖两个job el_crm和el_hyf，执行命令 是sh scripts/testb.sh ${azkaban.job.id} ${firstName}
    
    
	2.2、*.job文件是对特殊文件处理，最终被复制到发布的项目里面去
	
	特殊的job文件没办法通过.csv文件简单配置，可以单独编辑一个.job文件。
	项目生成后该job文件会把生成的简单job文件覆盖掉
	
	2.3、dw.properties项目参数配置，最终会被输出到system.properties
	
	
	2.4、scripts 存放脚本，最终也会被复制到项目里面去
	


# **temp目录**

    项目生成的临时文件夹，例如dw是用来存放项目dw的临时文件
    
    （包含job文件、脚本文件等）,最后会被打包成dw.zip文件,然后远程上传项目
    
    也可以单独指定备份目录. eg: config.ini temp_path=/home/xzh/azkaban/temp



aztest是测试文件


# azrun.py是命令工具文件


    /xuzh/azkaban/azkabans/azrun.py -h
    usage: azrun.py [-h] [-f FLOWS] [-t CRONTAB] [-i IGNORE] [-p PARAM]
                    {u,e,s,a} prj_nm
    
    远程部署azkaban
    
    positional arguments:
      {u,e,s,a}             u 表示更新项目元数据， e 表示执行项目 s 表示给项目添加执行计划,a 激活执行节点
      prj_nm                项目名称字符型
    
    optional arguments:
      -h, --help            show this help message and exit
      -f FLOWS, --flows FLOWS
                            工作流的字符列表,eg: "['a','b']" 设置后值执行这个列表里面的工作流
      -t CRONTAB, --crontab CRONTAB
                            cron的定时器格式字符串  0 5 0/5 * * *
      -i IGNORE, --ignore IGNORE
                            job name的字符列表 eg."['a','b']" 设置后跳过
      -p PARAM, --param PARAM
                            参数传入，数据字典,可以覆盖全局参数 "{'s':1}" 设置后执行flow时会覆盖系统参数
                           
# 快速入门
    1、拷贝azkabans项目到本地
    2、配置全局参数文件conf/config.ini
    配置完成后，可以执行aztest 中 test_login（）函数，测试配置情况
    3、开始配置项目mydw
        3.1 在conf创建项目文件夹mydw
        
        3.2 创建 mydw.csv，并开始配置job的命令和依赖（conf/mydw目录下）
        
        3.3 配置本项目参数mydw.properties文件（conf/mydw目录下）
            # crom 表示配置定时器（【必须配置】）com.prj_nm表示给项目所有的flow统一设置定时器，crom.flowid表示给flowid设置定时器（会覆盖com.prj_nm配置）
            cron.mydw=0 0/20 * * * ?    # 项目定时器 
            cron.endflow=0 0/30 * * * ?   # id 为 endflow 的flow的定时器 会覆盖cron.mydw定时器
            etl_ts=${azkaban.flow.start.year}-${azkaban.flow.start.month}-${azkaban.flow.start.day}
            
            
        3.4 azrun.py u mydw 这样就会把mydw项目上传到azkaban
        
        3.5 azrun.py s mydw 给mydw加定时任务
        
        3.6 azrun.py e mydw "{'useExecutor':2}" 执行mydw所有工作流，并且指定useExecutor

    4、aztest.py 用于开发测试，可以随意修改
    
# 总结
    1、项目里面azkaban,所有的操作api均已实现，可以看具体的对象方法
    2、 razrun.py 只支持 u 表示更新项目元数据， e 表示执行项目 s 表示给项目添加执行计划,a 激活执行节点
    3、对于指定执行任务暂没有实现，因为在操作界面上更容易
    4、没有做成web模式，因为感觉这样更简单


# 贡献代码
xuzh
 