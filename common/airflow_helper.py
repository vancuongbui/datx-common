import json


class AirflowHelper:
    @staticmethod
    def return_result(value, return_path: str = "/airflow/xcom/return.json"):
        json_str = json.dumps(value)
        f = open(return_path, "w")
        f.write(json_str)
        f.close()
