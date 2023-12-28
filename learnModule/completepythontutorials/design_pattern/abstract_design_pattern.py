from file_checks_project.ups1.CrefTestPack import CrefTestPack
from file_checks_project.ups2.GatewayCheck import GatewayCheck

hdfs_path = "hdfs_location"
table_name = "d_fin_table"
location = 'GatewayCheck'

argument_list = {'hdfs_file': hdfs_path, 'table_name': table_name, 'location': location}

# class TEST:
#     def __str__(self):
#         return 'TEST'
#
#     def foo(self):
#         print("this is test class foo method")
location = 'GATEWAY'


class InitCheck:

    def __init__(self, check=None):
        self.init_checks = check

    def run(self):
        print(self.init_checks)
        run_test = self.init_checks()
        run_test.run_test()


def get_check(checks_name='GATEWAY'):
    return GatewayCheck(argument_list)


print(get_check())
demo = InitCheck(get_check)
demo.run()
