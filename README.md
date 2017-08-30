开发初衷：
解决两个问题， 一是分布式爬虫为了能够协调彼此的爬取范围以及存储爬取的数据，需要个服务器来充当协调以及存储的角色，不同的分布式爬虫差别点只是解析网页的方法以及存储数据的格式不一样，其它均相同， 能否有个统一的框架；
二是对于分布式爬虫如果改动并重新部署到各个客户端，非常的麻烦， 能否一次部署之后，接下来可以通过服务器来控制和更新。

解决方案：
对于问题一：服务器端侧提供一个统一的接口用来满足数据的流进流出，流出每个客户端需要爬取的数据，流进客户端爬取的结果，不同的数据用请求参数table加以区分。服务器对每条流出以及流进的数据用state加以标识，0(未处理) 1(流出) 2(流进）；
客户端侧用一个基础类来完成与服务器的数据交互，子类完成各自的网页解析。
对于问题二：可以有一个占位的客户端，定期的从服务端读取命令，下载之后开辟新的进程去执行下载的工程即可，占位进程可以通过子进程的句柄对子进程实行控制。

构成：Server(中央服务器)， Troy(客户端占位程序)， Client(客户端爬虫程序)

特点：
1. 一遍部署之后，无需再手动部署
2. 对于需要爬取某个特定网站的分布式爬虫， 只需要编写特定的客户端网页解析代码
3. 数据存储无需在服务器手动创建表，服务器会自动根据客户端送上来的数据格式和表名称自动创建
4. 无论服务器和客户端都可以随时停止，修改并重新运行，客户端遇到与服务器沟通出现问题时，会暂停一会后继续沟通。客户端会缓存下载后还未将解析数据送至服务器的网页，这样即使客户端意外崩溃，继续进行的时候不会重新去爬取它们
5. 每个客户端的日志在有网的情况下自动上传到服务器


安装步骤：
    1.安装环境：
        服务器端：
            python2.x
            mysql
        客户端：
            Requests
            bs4


    2. 启动Server:
        运行mysql, 并创建db
        配置数据库连接： 在Server/common_db_oper.py中
        设置一个合适的端口号， 在Server/main.py中
        切换到Server文件夹运行 python main.py


    3. 启动Troy:
        可以运行在Windows和Mac电脑上， Linux上如何终止一个进程还没有得到解决
        Troy可以部署到多台机器上， 因此每个终端为了区分，需要设置一个唯一的终端名称
        设置Troy要连接的服务器地址， 在Troy/main.py中
        切换到Troy文件夹下：执行 python main.py <client_name>
        这样Troy便与服务器取得联系，并让服务器自动创建了client_control表格用来记录Troy的状态， 在这个表格中可以操纵Troy去做事情， 通过设置cmd字段， 目前支持以下四个命令：
            dr:<xxx.zip>, 下载一个zip包并运行里面的程序， zip包里需包含main.py文件， zip包的默认路径在部署的服务器的static目录下
            t:, 终止troy加载运行的子程序
            restart:, 重启troy加载的子程序
            c:, 清除troy加载的子程序的所有相关文件

    4. 创建自己的Client, 参考两个Demo, china_brand(中华品牌网)， poi_86(中国poi网站)
       在Client/main.py中设置想要运行的程序，将Client整个文件夹压缩成zip包， 放置到服务器的static目录下
       在数据库的client_control表格中给想要的客户端设置cmd字段如：dr:xxx.zip， 则对应的客户端便会在10s内下载并运行指定的程序，子程序的log会上传到服务器的runtime目录下
