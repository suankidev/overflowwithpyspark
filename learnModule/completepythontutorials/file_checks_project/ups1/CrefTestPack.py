from file_checks_project.common.RunTest import RunTest


class CrefTestPack(RunTest):

    def create_stg_df(self):
        pass

    def create_final_df(self):
        pass

    def create_file_df(self):
        print("ups1 file df is created!")

    def run_test(self):
        self.create_file_df()

    def __init__(self, args_list):
        self.argument_list = args_list

    def __str__(self):
        return 'CrefTestPack'


# obj = CrefTestPack('hdfs_file_path', 'd_fin_table')
