import sys
import time
import concurrent.futures
import argparse
from dataclasses import dataclass, field
from settings import RPM_TIMEOUT
from vk_api import VkApi


@dataclass
class HandshakesData:
    handshake: int = 1
    first_id: int = None
    last_id: int = None
    first_circle: list = field(default_factory=list)
    last_circle: list = field(default_factory=list)


not_verified_users_first_id = set()
not_verified_users_last_id = set()

verified_users_1 = {}
verified_users_2 = {}


class FiveHandshakes:
    def __init__(self, vk_api=VkApi(), max_handshakes=5):
        self.start_time = time.time()
        self.max_handshakes = max_handshakes
        self.vk_api = vk_api

    @staticmethod
    def _checkFriendsList(friends_list, parent_id, list_of_id, verified_dict):
        """
        :param friends_list: Полученный список друзей
        :param parent_id: Родитель этого списка
        :param list_of_id: Список, в который будут добавлены все полученные друзья
        :param verified_dict: Словарь, куда мы добавим всех полученных друзей с потомком
        :return: None
        """
        for friend in friends_list:
            # Исключаем добавление в поиск id, который уже был проверен
            if friend not in verified_dict:
                verified_dict[friend] = parent_id
                list_of_id.append(friend)

    def _collectPathFromTargetIds(self, data, mutual_friends):
        path = []
        if data.handshake == 2:
            path.append(mutual_friends[0])
        elif data.handshake == 3:
            path.append(mutual_friends[0])
            path.insert(0, verified_users_1[mutual_friends[0]])
        elif data.handshake == 4:
            path.append(mutual_friends[0])
            path.insert(0, verified_users_1[mutual_friends[0]])
            path.insert(2, verified_users_2[mutual_friends[0]])
        elif data.handshake == 5:
            path.append(mutual_friends[0])
            path.insert(0, verified_users_1[verified_users_1[mutual_friends[0]]])
            path.insert(1, verified_users_1[mutual_friends[0]])
            path.insert(3, verified_users_2[mutual_friends[0]])
        path.insert(0, data.first_id)
        path = path + [data.last_id]
        return path

    def _checkMutualFriends(self, mutual_friends, data):
        """
        Проверяем, есть ли общие друзья.
        :param mutual_friends: Список общих друзей
        :return:
        """
        if mutual_friends:
            path = self._collectPathFromTargetIds(data, mutual_friends)
            print("Они знакомы через {count} рукопожатия! \n Путь: {path}".format(count=len(path) - 1,
                                                                                  path=' -> '.join(str(i) for i in path)))
            print("Поиск занял: %s секунд" % (time.time() - self.start_time))
            sys.exit()

    @staticmethod
    def _getMutualFriends(list1, list2):
        """
        :param list1: List
        :param list2: List
        :return: Список общих элементов в 2х списках
        """
        return list(set(list1) & set(list2))

    def confirmTheoryOfAnyHandshakes(self, first_id, last_id, max_lap):
        handshakes_data = HandshakesData(first_id=first_id, last_id=last_id)
        self.max_handshakes = max_lap
        print("Добавляем первого и последнего юзера в списки для поиска друзей")
        not_verified_users_first_id.add(handshakes_data.first_id)
        not_verified_users_last_id.add(handshakes_data.last_id)

        print("Проверяем на 1 рукопожатие")
        self._checkOneHandshake(handshakes_data)
        handshakes_data.handshake += 1

        print("Получаем список друзей второго юзера и ищем общих друзей обоих юзеров")
        self._findFriends(not_verified_users_last_id, handshakes_data.last_circle, verified_users_2)
        self._checkMutualFriends(self._getMutualFriends(handshakes_data.first_circle,
                                                        handshakes_data.last_circle), handshakes_data)
        handshakes_data.handshake += 1

        print("Добавляем найденых друзей каждого юзера в список для получения друзей")
        for friend in handshakes_data.first_circle:
            not_verified_users_first_id.add(friend)
        for friend in handshakes_data.last_circle:
            not_verified_users_last_id.add(friend)
        handshakes_data.first_circle = []

        print("Получаем всех друзей друзей первого юзера и ищем общих с друзьями второго юзера")
        self._checkAnyHandshake(handshakes_data, not_verified_users_first_id,
                                handshakes_data.first_circle, verified_users_1)
        handshakes_data.handshake += 1
        handshakes_data.last_circle = []

        print("Получаем всех друзей друзей второго юзера и ищем общих друзей с друзьями друзей первого юзера")
        self._checkAnyHandshake(handshakes_data, not_verified_users_last_id,
                                handshakes_data.last_circle, verified_users_2)
        handshakes_data.handshake += 1

        print("Добавляем всех друзей друзей первого юзера в список для получения друзей")
        for friend in handshakes_data.first_circle:
            not_verified_users_first_id.add(friend)
        handshakes_data.first_circle = []

        print("Получаем всех друзей друзей друзей первого юзера и ищем общих с друзьями друзей второго юзера")
        self._checkAnyHandshake(handshakes_data, not_verified_users_first_id,
                                handshakes_data.first_circle, verified_users_1)
        print("Они не знакомы через 5 рукопожатий")

    def _getFriendsWorker(self, queue, list_of_id, verified_dict, data):
        """
        Запускает потоки по получению друзей из очереди id
        :param queue: Множество id чьих друзей нам нужно получить
        :param list_of_id: Список, в который будут добавлены все полученные друзья
        :param verified_dict: Словарь, куда мы добавим всех полученных друзей с потомком
        :return: None
        """
        step = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            while queue:
                # Чтобы не ждать обработки всех 10000 id, каждые 20 мы будем отправлять на проверку общих друзей
                if step == 20:
                    self._checkMutualFriends(self._getMutualFriends(set(data.first_circle),
                                                                    set(data.last_circle)), data)
                    step = 0
                time.sleep(RPM_TIMEOUT)
                executor.submit(self._findFriends, queue.pop(), list_of_id, verified_dict)
                step += 1

    def _findFriends(self, item, set_of_id, verified_dict):
        """
        :param item:
        :param set_of_id:
        :param verified_dict:
        :return:
        """

        friends = self.vk_api.getFriends(item)
        if friends:
            self._checkFriendsList(friends['items'], item, set_of_id, verified_dict)

    def _checkOneHandshake(self, data):
        self._findFriends(not_verified_users_first_id, data.first_circle, verified_users_1)
        self._checkMutualFriends(self._getMutualFriends(data.first_circle, [data.last_id]), data)

    def _checkAnyHandshake(self, data, not_verified_users, circle_list, verified_dict):
        self._getFriendsWorker(not_verified_users, circle_list, verified_dict, data)
        self._checkMutualFriends(self._getMutualFriends(set(data.first_circle), set(data.last_circle)), data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("first_id")
    parser.add_argument("last_id")
    parser.add_argument("max_handshakes")
    args = parser.parse_args()
    FiveHandshakes().confirmTheoryOfAnyHandshakes(args.first_id, args.last_id, args.max_handshakes)
