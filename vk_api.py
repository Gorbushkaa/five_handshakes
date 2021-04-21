import requests
from settings import TOKEN


FRIENDS_GET_API_URL = "https://api.vk.com/method/friends.get"


class VkApi:
    def getFriends(self, id):
        response = requests.get(
            FRIENDS_GET_API_URL,
            params={'user_id': id, 'access_token': TOKEN, 'v': '5.52'}).json()
        return response.get('response', [])


