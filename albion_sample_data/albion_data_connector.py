import json
import requests


# ! Utilizes https://albion-online-data.com/ API to retrieve market data information
class AlbionDataFetcher:
    def __init__(self):
        self.url = (
            "https://albion-online-data.com/api/v2/stats/history/T7_MEAL_OMELETTE_AVALON,T7_MEAL_OMELETTE_AVALON@1,T7_MEAL_OMELETTE_AVALON@2,T7_MEAL_OMELETTE_AVALON@3,T7_MEAL_OMELETTE_FISH,T7_MEAL_OMELETTE_FISH@1,T7_MEAL_OMELETTE_FISH@2,T7_MEAL_OMELETTE_FISH@3,T7_MEAL_OMELETTE,T7_MEAL_OMELETTE@1,T7_MEAL_OMELETTE@2,T7_MEAL_OMELETTE@3,T7_MEAL_PIE_FISH,T7_MEAL_PIE_FISH@1,T7_MEAL_PIE_FISH@2,T7_MEAL_PIE_FISH@3,T7_MEAL_PIE,T7_MEAL_PIE@1,T7_MEAL_PIE@2,T7_MEAL_PIE@3,T6_MEAL_SALAD_FISH,T6_MEAL_SALAD_FISH@1,T6_MEAL_SALAD_FISH@2,T6_MEAL_SALAD_FISH@3,T6_MEAL_SALAD,T6_MEAL_SALAD@1,T6_MEAL_SALAD@2,T6_MEAL_SALAD@3,T6_MEAL_SANDWICH_AVALON,T6_MEAL_SANDWICH_AVALON@1,T6_MEAL_SANDWICH_AVALON@2,T6_MEAL_SANDWICH_AVALON@3,T8_MEAL_SANDWICH,T8_MEAL_SANDWICH@1,T8_MEAL_SANDWICH@2,T8_MEAL_SANDWICH@3,T8_MEAL_SANDWICH_FISH,T8_MEAL_SANDWICH_FISH@1,T8_MEAL_SANDWICH_FISH@2,T8_MEAL_SANDWICH_FISH@3,T5_MEAL_SOUP_FISH,T5_MEAL_SOUP_FISH@1,T5_MEAL_SOUP_FISH@2,T5_MEAL_SOUP_FISH@3,T8_MEAL_STEW_AVALON,T8_MEAL_STEW_AVALON@1,T8_MEAL_STEW_AVALON@2,T8_MEAL_STEW_AVALON@3,T8_MEAL_STEW,T8_MEAL_STEW@1,T8_MEAL_STEW@2,T8_MEAL_STEW@3,T8_MEAL_STEW_FISH,T8_MEAL_STEW_FISH@1,T8_MEAL_STEW_FISH@2,T8_MEAL_STEW_FISH@3,T7_MEAL_ROAST,T7_MEAL_ROAST@1,T7_MEAL_ROAST@2,T7_MEAL_ROAST@3,T7_MEAL_ROAST_FISH,T7_MEAL_ROAST_FISH@1,T7_MEAL_ROAST_FISH@2,T7_MEAL_ROAST_FISH@3,T8_MILK,T7_MEAT,T5_EGG,QUESTITEM_TOKEN_AVALON,T7_FISH_FRESHWATER_STEPPE_RARE,T7_CORN,T7_MULLEIN,T7_FISH_FRESHWATER_MOUNTAIN_RARE,T3_FLOUR,T6_MILK,T1_FISHSAUCE_LEVEL1,T1_FISHSAUCE_LEVEL2,T1_FISHSAUCE_LEVEL3,T7_FISH_SALTWATER_ALL_RARE,T6_POTATO,T6_MEAT,T5_CABBAGE,T8_MEAT,T8_BUTTER,T8_PUMPKIN,T8_YARROW,T5_MEAT,T5_TEASEL,T4_BREAD,T7_FISH_FRESHWATER_AVALON_RARE,T7_FISH_FRESHWATER_FOREST_RARE,T7_FISH_FRESHWATER_SWAMP_RARE,T7_FISH_FRESHWATER_HIGHLANDS_RARE,T6_FOXGLOVE?time-scale=24")

    def get_request(self):
        """Send a GET request to the URL, save the JSON response to a file, and return JSON object."""
        response = requests.get(self.url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to retrieve Albion Online Data: {response.status_code}")

# #? Runtime test
# data_connector = AlbionDataFetcher()
# data = data_connector.get_request()
#
# print(json.dumps(data, indent=4))
