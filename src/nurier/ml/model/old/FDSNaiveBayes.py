from sklearn.naive_bayes import BernoulliNB

from src.nurier.ml.model.old.FDSModel import FDSModel
from src.nurier.ml.common.CommonUtil import CommonUtil as util
from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.fds.common.LoggingHandler import LoggingHandler

logger = LoggingHandler(f"{prop().get_result_log_file_path()}{util().now_type3()}_FDSNaiveBayes", "a", "DEBUG")


class FDSNaiveBayes(FDSModel):

    def __init__(self, data):
        self.model = BernoulliNB()
        self.log = logger.get_log()
        super().__init__(self.log, self.model, data)

    def set_model(self):
        self.model = BernoulliNB()
