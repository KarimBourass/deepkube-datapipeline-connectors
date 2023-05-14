import requests
import pandas as pd

from connectors.api_connectors.api_connector import APIConnector


class GetAPIConnecot(APIConnector):

    def __init__(self,  **kwargs):
        super().__init__(kwargs)

    def get_df(self, *args, **kwargs):
        response = requests.get(kwargs["url"])
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame.from_records(data)
        return df
