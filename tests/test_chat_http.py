import ast
import json
import logging
import re
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from aiohttp.test_utils import TestClient, TestServer
from chat_http import create_chat_app


class Cursor:
    def __init__(self, pool):
        self.pool = pool
        self.result = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def execute(self, query, params):
        self.pool.queries.append((query, params))
        if 'FROM clients' in query and 'room_messages' not in query:
            self.result = self.pool.user
        elif 'COUNT(*)' in query:
            self.result = self.pool.rooms
        elif 'FROM room_participants' in query:
            self.result = self.pool.participant
        else:
            self.result = self.pool.messages[:params[-1]]

    async def fetchone(self):
        return self.result

    async def fetchall(self):
        return self.result


class Connection:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def cursor(self, *args):
        return Cursor(self.pool)


class Pool:
    def __init__(self):
        self.user = {'id': 7, 'username': 'george'}
        self.participant = {'last_message_seen': 10}
        self.rooms = [
            {'room': 1, 'name': 'Ops', 'room_type': 'group', 'last_message_at': datetime(2026, 10, 19, 9, 0),
             'last_message_seen': 10, 'last_message_id': 12, 'unread_count': 2},
            {'room': 2, 'name': None, 'room_type': 'direct', 'last_message_at': None,
             'last_message_seen': 4, 'last_message_id': 4, 'unread_count': 0},
        ]
        self.messages = [
            {'id': 11, 'user_id': 3, 'username': 'ann', 'room_id': 1, 'message': 'hi', 'message_information': None,
             'created_at': datetime(2026, 10, 19, 9, 0), 'updated_at': datetime(2026, 10, 19, 9, 0)},
            {'id': 12, 'user_id': 3, 'username': 'ann', 'room_id': 1, 'message': 'there', 'message_information': None,
             'created_at': datetime(2026, 10, 19, 9, 1), 'updated_at': datetime(2026, 10, 19, 9, 1)},
        ]
        self.queries = []

    def acquire(self):
        return Connection(self)


CHAT_OPERATIONS = sorted(set(re.findall(r'\bchat\.(\w+)', Path('chat_http.py').read_text())))


class ChatHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pool = Pool()
        self.chat = SimpleNamespace(**{name: AsyncMock() for name in CHAT_OPERATIONS})
        self.chat.is_user_online = Mock(return_value=True)
        self.chat.get_users_in_room.return_value = [7, 3, 4]
        self.chat.store_new_message.return_value = 13
        self.chat.edit_message_in_room.return_value = 1
        self.chat.delete_message_in_room.return_value = True
        self.chat.create_or_update_room.return_value = (5, 'group', None)
        app = create_chat_app(self.pool, 'test-secret', logging.getLogger('test-chat'), self.chat)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def get(self, path, secret='test-secret', **params):
        query = {'username': 'george', 'organization_id': '1', **params}
        query = {key: value for key, value in query.items() if value is not None}
        return await self.client.get(path, params=query, headers={'Authorization': 'Bearer ' + secret})

    async def post(self, path, body=None, secret='test-secret'):
        body = {'username': 'george', 'organization_id': 1, **(body or {})}
        return await self.client.post(path, json=body, headers={'Authorization': 'Bearer ' + secret})

    async def test_authentication_before_database_access(self):
        response = await self.get('/chat/unread', secret='incorrect')
        self.assertEqual(response.status, 401)
        self.assertEqual(self.pool.queries, [])

    async def test_post_is_not_allowed(self):
        response = await self.client.post('/chat/unread', headers={'Authorization': 'Bearer test-secret'})
        self.assertEqual(response.status, 405)

    async def test_invalid_organization_and_username(self):
        self.assertEqual((await self.get('/chat/unread', organization_id='0')).status, 400)
        self.assertEqual((await self.get('/chat/unread', organization_id='-1')).status, 400)
        self.assertEqual((await self.get('/chat/unread', username=' ')).status, 400)
        self.assertEqual(self.pool.queries, [])

    async def test_unknown_user(self):
        self.pool.user = None
        response = await self.get('/chat/unread')
        self.assertEqual(response.status, 404)
        self.assertEqual(await response.json(), {'error': 'user_not_found'})

    async def test_unread_summary(self):
        response = await self.get('/chat/unread')
        self.assertEqual(response.status, 200)
        body = await response.json()
        self.assertTrue(body['has_new_messages'])
        self.assertEqual(body['total_unread'], 2)
        self.assertEqual(body['rooms'][0]['last_message_at'], '2026-10-19T09:00:00')
        # The user lookup and the summary are both confined to the requested organization.
        self.assertEqual(self.pool.queries[0][1], ('george', 1))
        self.assertEqual(self.pool.queries[1][1], (7, 1))

    async def test_no_unread(self):
        for room in self.pool.rooms:
            room['unread_count'] = 0
        body = await (await self.get('/chat/unread')).json()
        self.assertFalse(body['has_new_messages'])
        self.assertEqual(body['total_unread'], 0)

    async def test_messages_default_to_last_seen(self):
        response = await self.get('/chat/messages', room='1')
        self.assertEqual(response.status, 200)
        body = await response.json()
        self.assertEqual(body['last_id'], 10)
        self.assertEqual(body['next_last_id'], 12)
        self.assertFalse(body['has_more'])
        self.assertEqual([m['id'] for m in body['messages']], [11, 12])
        self.assertEqual(self.pool.queries[-1][1], (1, 1, 10, 21))

    async def test_messages_after_explicit_last_id_with_more(self):
        body = await (await self.get('/chat/messages', room='1', last_id='0', limit='1')).json()
        self.assertTrue(body['has_more'])
        self.assertEqual(body['next_last_id'], 11)
        self.assertEqual(len(body['messages']), 1)
        self.assertEqual(self.pool.queries[-1][1], (1, 1, 0, 2))

    async def test_limit_is_capped(self):
        await self.get('/chat/messages', room='1', limit='5000')
        self.assertEqual(self.pool.queries[-1][1][-1], 101)

    async def test_no_new_messages_keeps_cursor(self):
        self.pool.messages = []
        body = await (await self.get('/chat/messages', room='1', last_id='12')).json()
        self.assertEqual(body['messages'], [])
        self.assertEqual(body['next_last_id'], 12)

    async def test_non_participant_is_forbidden(self):
        self.pool.participant = None
        response = await self.get('/chat/messages', room='1')
        self.assertEqual(response.status, 403)
        self.assertFalse(any('room_messages' in query for query, _ in self.pool.queries))

    async def test_invalid_message_parameters(self):
        self.assertEqual((await self.get('/chat/messages')).status, 400)
        self.assertEqual((await self.get('/chat/messages', room='abc')).status, 400)
        self.assertEqual((await self.get('/chat/messages', room='1', last_id='-5')).status, 400)
        self.assertEqual((await self.get('/chat/messages', room='1', limit='0')).status, 400)

    async def test_database_failure(self):
        async def fail(*args):
            raise RuntimeError('database unavailable')
        Cursor.execute, original = fail, Cursor.execute
        try:
            response = await self.get('/chat/unread')
        finally:
            Cursor.execute = original
        self.assertEqual(response.status, 500)
        self.assertEqual(await response.json(), {'error': 'request_failed'})

    async def test_post_authentication_and_body_validation(self):
        response = await self.post('/chat/rooms/1/messages', {'message': 'hi'}, secret='incorrect')
        self.assertEqual(response.status, 401)
        self.chat.store_new_message.assert_not_called()
        response = await self.client.post('/chat/rooms', data='not json',
                                          headers={'Authorization': 'Bearer test-secret'})
        self.assertEqual(response.status, 400)
        response = await self.client.post('/chat/rooms', json=[1],
                                          headers={'Authorization': 'Bearer test-secret'})
        self.assertEqual(await response.json(), {'error': 'expected_json_object'})

    async def test_room_endpoints_require_participation(self):
        self.pool.participant = None
        for path in ('/chat/rooms/1/messages', '/chat/rooms/1/messages/2/edit', '/chat/rooms/1/messages/2/delete',
                     '/chat/rooms/1/leave', '/chat/rooms/1/last-seen'):
            response = await self.post(path, {'message': 'hi', 'msg_id': 1})
            self.assertEqual(response.status, 403, path)
        for path in ('/chat/rooms/1/users', '/chat/rooms/1/messages/latest'):
            self.assertEqual((await self.get(path)).status, 403, path)
        self.chat.store_new_message.assert_not_called()
        self.chat.leave_room.assert_not_called()
        self.chat.get_user_names_in_room.assert_not_called()

    async def test_room_scope_uses_organization(self):
        self.chat.get_user_names_in_room.return_value = []
        self.chat.get_room_owner.return_value = []
        self.assertEqual((await self.get('/chat/rooms/1/users')).status, 200)
        self.assertEqual(self.pool.queries[-1][1], (7, 1, 1))

    async def test_send_message_stores_and_relays_like_websocket(self):
        response = await self.post('/chat/rooms/1/messages', {'message': 'hello', 'msginfo': '{"type":"text"}'})
        self.assertEqual(await response.json(), {'success': True, 'msgid': 13})
        self.chat.store_new_message.assert_awaited_once_with(self.pool, 7, 'hello', '{"type":"text"}', 1, 1)
        pool, payload, user_ids, organization_id, room_id = self.chat.send_msg_to_users.await_args.args
        # The sender is excluded, as for BroadcastMessage.
        self.assertEqual((user_ids, organization_id, room_id), ([3, 4], 1, 1))
        self.assertEqual(json.loads(payload), {'event': 'chat_message', 'data': {
            'username': 'george', 'msgid': 13, 'room': 1, 'message': 'hello', 'msginfo': '{"type":"text"}'}})

    async def test_send_message_validation(self):
        self.assertEqual((await self.post('/chat/rooms/1/messages', {})).status, 400)
        self.assertEqual((await self.post('/chat/rooms/1/messages', {'message': 'x', 'msginfo': {'a': 1}})).status, 400)
        self.assertEqual((await self.post('/chat/rooms/x/messages', {'message': 'x'})).status, 400)
        self.chat.store_new_message.assert_not_called()

    async def test_edit_message_broadcasts_update(self):
        body = await (await self.post('/chat/rooms/1/messages/12/edit', {'message': 'new', 'msginfo': None})).json()
        self.assertEqual(body, {'success': True, 'msgid': 12})
        self.chat.edit_message_in_room.assert_awaited_once_with(self.pool, 7, 12, 'new', None, 1, 1)
        pool, payload, user_ids, *_ = self.chat.send_msg_to_users.await_args.args
        self.assertEqual(json.loads(payload)['event'], 'chat_message_updated')
        self.assertEqual(user_ids, [7, 3, 4])

    async def test_failed_edit_does_not_broadcast(self):
        self.chat.edit_message_in_room.return_value = 0
        body = await (await self.post('/chat/rooms/1/messages/12/edit', {'message': 'new'})).json()
        self.assertFalse(body['success'])
        self.chat.send_msg_to_users.assert_not_called()

    async def test_delete_message(self):
        body = await (await self.post('/chat/rooms/1/messages/12/delete')).json()
        self.assertEqual(body, {'success': True, 'msgid': 12})
        self.chat.delete_message_in_room.assert_awaited_once_with(self.pool, 7, 1, 12, 1)

    async def test_rooms_and_room_users(self):
        self.chat.get_user_rooms.return_value = [{'id': 1, 'name': 'Ops'}]
        self.assertEqual(await (await self.get('/chat/rooms')).json(), {'rooms': [{'id': 1, 'name': 'Ops'}]})
        self.chat.get_user_names_in_room.return_value = [{'id': 3, 'username': 'ann', 'online': False}]
        self.chat.get_room_owner.return_value = [{'id': 7, 'username': 'george', 'online': True}]
        body = await (await self.get('/chat/rooms/1/users')).json()
        self.assertEqual(body['users'][0]['username'], 'ann')
        self.assertEqual(body['owners'][0]['username'], 'george')

    async def test_make_room(self):
        body = await (await self.post('/chat/rooms', {'name': 'Ops', 'users': ['ann', 4], 'description': 'd'})).json()
        self.assertEqual(body, {'status': 'success', 'room': 5, 'name': 'Ops', 'type': 'group'})
        self.chat.create_or_update_room.assert_awaited_once_with(self.pool, 7, 'Ops', ['ann', 4], 'd', 1, None)
        self.chat.create_or_update_room.return_value = (None, 'group', 'Only the room owner can update this room')
        response = await self.post('/chat/rooms', {'name': 'Ops', 'users': []})
        self.assertEqual(response.status, 422)
        self.assertEqual((await response.json())['error'], 'Only the room owner can update this room')
        self.assertEqual((await self.post('/chat/rooms', {'name': 'Ops', 'users': 'ann'})).status, 400)

    async def test_room_toggles_and_last_seen(self):
        for path, operation in (('leave', 'leave_room'), ('silent', 'silent_room'), ('unsilent', 'unsilent_room')):
            getattr(self.chat, operation).return_value = True
            body = await (await self.post('/chat/rooms/1/' + path)).json()
            self.assertEqual(body, {'success': True})
            getattr(self.chat, operation).assert_awaited_once_with(self.pool, 1, 7)
        self.chat.update_last_seen_msg_in_room.return_value = True
        self.assertTrue((await (await self.post('/chat/rooms/1/last-seen', {'msg_id': 12})).json())['success'])
        self.chat.update_last_seen_msg_in_room.assert_awaited_once_with(self.pool, 7, 1, 12, 1)
        self.assertEqual((await self.post('/chat/rooms/1/last-seen', {})).status, 400)
        self.assertEqual(await (await self.post('/chat/rooms/1/clear-last-seen')).json(), {'success': True})
        self.chat.clear_user_last_seen_msg.assert_awaited_once_with(self.pool, 7, 1)

    async def test_previous_and_latest_messages(self):
        self.chat.get_prev_messages_in_room.return_value = [{'id': 9}]
        body = await (await self.get('/chat/rooms/1/messages/previous', before_id='10')).json()
        self.assertEqual(body, {'room': 1, 'messages': [{'id': 9}]})
        self.chat.get_prev_messages_in_room.assert_awaited_once_with(self.pool, 7, 1, 1, 10)
        self.assertEqual((await self.get('/chat/rooms/1/messages/previous')).status, 400)
        self.chat.get_last_messages_in_room.return_value = [{'id': 12}]
        body = await (await self.get('/chat/rooms/1/messages/latest')).json()
        self.assertEqual(body['messages'], [{'id': 12}])

    async def test_room_path_new_messages(self):
        body = await (await self.get('/chat/rooms/1/messages', last_id='10')).json()
        self.assertEqual(body['next_last_id'], 12)

    async def test_ping_and_user_status(self):
        self.assertEqual(await (await self.get('/chat/ping')).json(), {'status': True, 'user_id': 7})
        self.assertEqual(await (await self.get('/chat/user-status')).json(), {'user_id': 7, 'online': True})

    async def test_notification(self):
        response = await self.post('/chat/notifications', {'title': 'T', 'body': 'B', 'data': {'notification': 'abc'}})
        self.assertEqual(await response.json(), {'success': True})
        pool, message, user_id, organization_id, title, body = \
            self.chat.send_general_notification_msg_to_users.await_args.args
        self.assertEqual(json.loads(message), {'event': 'notification',
                                               'data': {'title': 'T', 'body': 'B', 'message': 'abc'}})
        self.assertEqual((user_id, organization_id, title, body), (7, 1, 'T', 'B'))

    async def test_operation_failure(self):
        self.chat.store_new_message.side_effect = RuntimeError('database unavailable')
        response = await self.post('/chat/rooms/1/messages', {'message': 'hi'})
        self.assertEqual(response.status, 500)
        self.assertEqual(await response.json(), {'error': 'request_failed'})


class MainWiringTests(unittest.TestCase):
    def test_main_passes_every_chat_operation(self):
        tree = ast.parse(Path('main.py').read_text())
        defined = {node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and getattr(node.func, 'id', None) == 'SimpleNamespace']
        self.assertEqual(len(calls), 1)
        passed = {keyword.arg: keyword.value.id for keyword in calls[0].keywords}
        self.assertEqual(set(passed), set(CHAT_OPERATIONS))
        self.assertTrue(set(passed.values()) <= defined, set(passed.values()) - defined)


if __name__ == '__main__':
    unittest.main()
