from file_checks_project.ups1.CrefTestPack import CrefTestPack
from file_checks_project.ups2.GatewayCheck import GatewayCheck

hdfs_path = "hdfs_location"
table_name = "d_fin_table"
location = 'GATEWAY'

argument_list = {'hdfs_file': hdfs_path, 'table_name': table_name, 'location': location}


def init_check(checks='Default'):
    run_test = {
        'GATEWAY': GatewayCheck,
        'CREF': CrefTestPack
    }

    return run_test[checks](argument_list)


init_check(location).run_test()
