from file_checks_project.common.RunTest import RunTest


class GatewayCheck(RunTest):

    def create_stg_df(self):
        pass

    def create_final_df(self):
        pass

    def create_file_df(self):
        print("gw file df is created!")

    def run_test(self):
        self.create_file_df()

    def __init__(self, args_list):
        self.argument_list = args_list

    def __str__(self):
        return 'GatewayCheck'


