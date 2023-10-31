from src.nurier.ml.event.PredictionEvent import PredictionEvent
from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from rediscluster import RedisCluster
import json
import time


class MLStorageSender:

    __instance = None

    def __init__(self):
        server_list = prop().get_save_engine_ml_servers()
        startup_nodes = []
        for server in server_list:
            server_split = server.split(":")
            startup_nodes.append({"host": server_split[0], "port": server_split[1]})
        self.client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
        MLStorageSender.__instance = self

    @staticmethod
    def getInstance():
        if MLStorageSender.__instance is None:
            MLStorageSender()
        return MLStorageSender.__instance

    def run(self, *args):
        if args[0] is not None and isinstance(args[0], PredictionEvent):
            start_time = time.time()
            event: PredictionEvent = args[0]
            print(f"[MLStorageSender] result : {str(event.prediction_value)}")
            user_id = event.original_message['E_FNC_USRID']
            print(f"[MLStorageSender] user id : {str(user_id)}")

            # Redis에서 유저 정보 조회
            if self.client.exists(f"info_{user_id}"):
                # json.loads = JSON(string) -> dictionary
                user_info = json.loads(self.client.get(f"info_{user_id}"))
                print(f"[MLStorageSender] user info : {user_info}")

                # Redis에 유저 정보 저장
                # json.dumps = dictionary -> JSON(string)
                user_info['mlresult'] = str(event.prediction_value)
                self.client.set(f"info_{user_id}", json.dumps(user_info))
            else:
                user_info = {'mlresult': str(event.prediction_value)}
                self.client.set(f"info_{user_id}", json.dumps(user_info))
            print(f"[MLStorageSender] time : {time.time() - start_time:.5f}sec")

        elif args[0] is not None and isinstance(args[0], list):
            start_time = time.time()
            event_list = args[0]
            for event_message in event_list:
                print(f"[MLStorageSender] prediction value : {str(event_message.prediction_value)}")
                user_id = event_message.original_message['E_FNC_USRID']
                print(f"[MLStorageSender] user id : {str(user_id)}")

                # Redis에서 유저 정보 조회
                if self.client.exists(f"info_{user_id}"):
                    # json.loads = JSON(string) -> dictionary
                    user_info = json.loads(self.client.get(f"info_{user_id}"))
                    print(f"[MLStorageSender] user info : {user_info}")

                    # Redis에 유저 정보 저장
                    # json.dumps = dictionary -> JSON(string)
                    user_info['mlresult'] = str(event_message.prediction_value)
                    self.client.set(f"info_{user_id}", json.dumps(user_info))
                else:
                    user_info = {'mlresult': str(event_message.prediction_value)}
                    self.client.set(f"info_{user_id}", json.dumps(user_info))

            print(f"[MLStorageSender] size : {len(event_list)}")
            print(f"[MLStorageSender] time : {time.time() - start_time:.5f}sec")
        else:
            print(f"[MLStorageSencer] args : {args}")
