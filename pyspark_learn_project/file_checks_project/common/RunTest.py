from abc import ABC, abstractmethod


class RunTest(ABC):
    @abstractmethod
    def create_stg_df(self):
        pass

    @abstractmethod
    def create_final_df(self):
        pass

    @abstractmethod
    def create_file_df(self):
        pass

    @abstractmethod
    def run_test(self):
        pass
