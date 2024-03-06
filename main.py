# -*- coding: utf-8 -*-

import datetime
import json
import asyncio
import copy
import vk_api
import json_file

from os.path import dirname, abspath
from pathlib import Path


dirname_root = Path(dirname(abspath(__file__)))
filename_settings = dirname_root / "settings.json"


def auth_handler():
    """
    При двухфакторной аутентификации вызывается эта функция.
    """
    key = input("Введи высланный тебе код авторизации: ")
    remember_device = True

    return key, remember_device


def captcha_handler(captcha):
    """
    При возникновении капчи вызывается эта функция и ей передается объект
    капчи. Через метод get_url можно получить ссылку на изображение.
    Через метод try_again можно попытаться отправить запрос с кодом капчи
    """
    key = input("Введи каптчу {0}: ".format(captcha.get_url())).strip()

    # Пробуем снова отправить запрос с капчей
    return captcha.try_again(key)


async def main():

    async def process_thread(post):
        try:
            tasks = set()
            tasks.add(asyncio.create_task(process_post(post, True, post)))

            if search_comments and post['comments']['count'] > 0:
                tasks.add(asyncio.create_task(process_comments(post, post)))

            for sub_task in tasks:
                await sub_task

        except Exception as err_msg:
            await log_error(err_msg)

        finally:
            async with running_post_tasks_lock:
                task_obj = asyncio.current_task()
                if task_obj in running_post_tasks:
                    running_post_tasks.discard(task_obj)

    async def process_post(post, is_op, op_post):
        tasks = set()

        post_user_id = post['from_id']
        if is_target(post_user_id):
            if is_op:
                post_type = 'OP'
            else:
                post_type = 'comment'

            tasks.add(asyncio.create_task(user_post_found(post_user_id, post, post_type, op_post)))

        if search_likes and ('likes' in post) and (post['likes']['count'] > 0):
            if is_op:
                like_type = 'post'
            else:
                like_type = 'comment'

            likes = tools.get_all_iter('likes.getList', 100, {
                'type': like_type,
                'owner_id': -group_id,
                'item_id': post['id']
            })

            for like in likes:
                if is_target(like):
                    tasks.add(asyncio.create_task(user_post_found(like, post, 'like', op_post)))

        for sub_task in tasks:
            await sub_task

    def is_target(user_id):
        for target_id in target_ids:
            if user_id == target_id:
                return True
        return False

    async def process_comments(post, op_post):
        comments = tools.get_all_iter('wall.getComments', 100, {
            'owner_id': -group_id,
            'post_id': post['id'],
            'need_likes': search_likes,
            'preview_length': 0
        })

        tasks = set()
        for comment in comments:
            tasks.add(asyncio.create_task(process_post(comment, False, op_post)))

            if comment['thread']['count'] > 0:
                thread = tools.get_all_iter('wall.getComments', 100, {
                    'owner_id': -group_id,
                    'post_id': post['id'],
                    'need_likes': search_likes,
                    'preview_length': 0,
                    'comment_id': comment['id']
                })
                for thread_post in thread:
                    tasks.add(asyncio.create_task(process_post(thread_post, False, op_post)))

        for sub_task in tasks:
            await sub_task

    async def user_post_found(post_user_id, post, post_type, op_post):
        timestamp = post['date']
        date = str(datetime.datetime.fromtimestamp(timestamp))
        uid = f'#{post["id"]}: {date} - ({post_type})'
        log_entry = {
            "text": post['text'],
            "op_post": op_post['text']
        }

        this_user_results = results[str(post_user_id)]

        async with results_lock:
            if this_user_results['last_activity']['timestamp'] < timestamp:
                this_user_results['last_activity']['timestamp'] = timestamp
                this_user_results['last_activity']['string'] = date

            this_user_results['target_activity'][uid] = log_entry

    async def log_error(message):
        now = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
        full_message = f'{now}: EXCEPTION: {message}\n'
        async with error_lock:
            print('---------------------------------------------------')
            print(full_message)
            print('---------------------------------------------------')

            with open(error_filename, 'a') as errfile:
                errfile.write(full_message)

    def save_results(results_dict):
        if results_dict:
            results_txt = json.dumps(results_dict, indent=4, sort_keys=True, ensure_ascii=False)

            with open(results_filename, 'w', encoding='utf-8') as f:
                f.write(results_txt)

    async def show_info():
        last_activity_len = 0

        while parsing_in_process:
            results_dict = None
            async with results_lock:
                new_len = 0
                for target_id in target_ids:
                    new_len += len(results[str(target_id)]['target_activity'])

                if new_len > last_activity_len:
                    async with outfile_lock:
                        try:
                            save_results(results)
                            last_activity_len = new_len
                        except Exception as e:
                            print(e)

            print(f'{datetime.datetime.now()}, текущий пост: {post_count}, найдено: {new_len}')

            await asyncio.sleep(1)

    results_lock = asyncio.Lock()
    outfile_lock = asyncio.Lock()

    post_count = 0
    running_post_tasks = set()
    running_post_tasks_lock = asyncio.Lock()

    error_lock = asyncio.Lock()

    settings = json_file.JsonFile(filename_settings)
    login = settings['my_login']
    password = settings['my_password']
    user_addresses = settings['user_addresses']
    group_address = settings['group_address']
    results_filename = dirname_root / settings['results_filename']
    error_filename = dirname_root / settings['error_filename']
    search_likes = settings['search_likes']
    search_comments = settings['search_comments']

    with open(error_filename, 'w') as file:
        file.write('')

    TASK_POOL_SIZE = 1000

    if search_comments:
        TASK_POOL_SIZE = TASK_POOL_SIZE / 100

    if search_likes:
        TASK_POOL_SIZE = TASK_POOL_SIZE / 100

    if TASK_POOL_SIZE < 10:
        TASK_POOL_SIZE = 10

    results = {}

    try:
        print(f'Размер пула: {TASK_POOL_SIZE}')
        print(f'Авторизация...')
        vk_session = vk_api.VkApi(login, password, app_id=2685278, auth_handler=auth_handler, captcha_handler=captcha_handler)
        vk = vk_session.get_api()
        vk_session.auth()
        tools = vk_api.VkTools(vk_session)

        target_ids = []
        for user_addr in user_addresses:
            print(f'Находим id пользователя {user_addr}...')
            user_id = vk.utils.resolveScreenName(screen_name=user_addr)['object_id']

            print(f'Находим имя пользователя {user_addr}...')
            user_names = vk.users.get(user_id=user_id)
            first_name = user_names[0]['first_name']
            last_name = user_names[0]['last_name']

            target_ids.append(user_id)
            results[str(user_id)] = {
                'target': f'{user_addr} (id={user_id}), {first_name} {last_name}',
                'target_activity': {},
                'last_activity': {
                    'timestamp': 0,
                    'string': ''
                }
            }

        print(f'Находим id группы {group_address}...')
        group_id = vk.utils.resolveScreenName(screen_name=group_address)['object_id']
        results['location'] = f'{group_address} (id={group_id})'

        print('Получаем стену группы...')
        posts = tools.get_all_iter('wall.get', 100, {'owner_id': -group_id})

        print('Парсим посты...')

        save_results(results)
        parsing_in_process = True
        info_task = asyncio.create_task(show_info())

        for wall_post in posts:
            while True:
                async with running_post_tasks_lock:
                    running_task_count = len(running_post_tasks)

                if running_task_count < TASK_POOL_SIZE:
                    break

                await asyncio.sleep(0.5)

            post_count = post_count + 1
            task = asyncio.create_task(process_thread(wall_post))
            async with running_post_tasks_lock:
                running_post_tasks.add(task)

        for task in running_post_tasks:
            await task

        parsing_in_process = False
        await info_task

        print('Парсинг завершён.')

    except Exception as error_msg:
        await log_error(error_msg)

    save_results(results)

    print("Готово.")


if __name__ == '__main__':
    asyncio.run(main())
