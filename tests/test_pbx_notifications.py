import ast
import asyncio
import json
import logging
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from aiohttp.test_utils import TestClient, TestServer
from pbx_notifications import CallNotificationDispatcher, create_pbx_app


class Cursor:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def execute(self, query, params):
        self.pool.queries.append((query, params))
        if self.pool.failure:
            raise RuntimeError('database unavailable')

    async def fetchone(self):
        return self.pool.row


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
        self.row = {'device_token': '[]'}
        self.queries = []
        self.failure = False

    def acquire(self):
        return Connection(self)


class PbxNotificationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pool = Pool()
        self.tokens = Mock(return_value=[{'token': 'device-a'}])
        self.send = Mock(return_value='ok')
        self.payload = {'user_id': 2, 'organization_id': 1, 'call_id': 'call-123',
                        'caller_name': 'Test caller', 'caller_number': '100', 'extension': '200'}
        self.dispatcher = CallNotificationDispatcher(self.pool, self.tokens, self.send,
                                                     logging.getLogger('test-pbx'))
        self.app = create_pbx_app(self.dispatcher, 'test-secret', logging.getLogger('test-pbx'))
        self.client = TestClient(TestServer(self.app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def post(self, payload=None, secret='test-secret'):
        return await self.client.post('/pbx/notifications', json=self.payload if payload is None else payload,
                                      headers={'Authorization': 'Bearer ' + secret})

    async def test_authentication_before_database_access(self):
        response = await self.post(secret='incorrect')
        self.assertEqual(response.status, 401)
        self.assertEqual(self.pool.queries, [])
        self.send.assert_not_called()

    async def test_payload_validation(self):
        for field, value in [('user_id', True), ('organization_id', 0), ('call_id', ''),
                             ('call_id', None), ('caller_name', 7), ('caller_number', 'x' * 65), ('extension', 200)]:
            response = await self.post(dict(self.payload, **{field: value}))
            self.assertEqual(response.status, 400)
        self.assertEqual((await self.post([])).status, 400)
        response = await self.client.post('/pbx/notifications', data='invalid',
                                         headers={'Authorization': 'Bearer test-secret'})
        self.assertEqual(response.status, 400)
        response = await self.client.post('/pbx/notifications', data='x' * 9000,
                                         headers={'Authorization': 'Bearer test-secret'})
        self.assertEqual(response.status, 413)
        self.send.assert_not_called()

    async def test_organization_scoping_and_existing_app_payload(self):
        response = await self.post()
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.json())['accepted'], 1)
        query, params = self.pool.queries[0]
        self.assertIn('id = %s AND organization_id = %s', query)
        self.assertEqual(params, (2, 1))
        token, title, body, data, user_id, org_id = self.send.call_args.args
        self.assertEqual((token, title, body, user_id, org_id), ('device-a', 'Incoming call', 'Test caller', 2, 1))
        self.assertEqual(data['type'], 'incoming_call')
        self.assertEqual(data['call_id'], 'call-123')
        self.assertTrue(all(isinstance(value, str) for value in data.values()))
        duplicate = await self.post()
        self.assertTrue((await duplicate.json())['duplicate'])
        self.assertEqual(self.send.call_count, 1)

    async def test_null_caller_fields_are_treated_as_missing(self):
        response = await self.post(dict(self.payload, caller_name=None, extension=None))
        self.assertEqual(response.status, 200)
        data = self.send.call_args.args[3]
        self.assertEqual((data['caller'], data['caller_name'], data['extension']), ('100', '', ''))

    async def test_user_id_and_username_share_deduplication(self):
        self.pool.row = {'id': 2, 'device_token': '[]'}
        self.assertEqual((await self.post()).status, 200)
        payload = dict(self.payload, username='George')
        del payload['user_id']
        status, body = await self.dispatcher.dispatch(payload)
        self.assertEqual((status, body['duplicate']), (200, True))
        self.assertEqual(self.send.call_count, 1)

    async def test_retry_reaches_only_devices_that_failed(self):
        self.tokens.return_value = [{'token': 'device-a'}, {'token': 'device-b'}]
        self.send.side_effect = lambda token, *args: 'ok' if token == 'device-a' else 'error'
        result = await (await self.post()).json()
        self.assertEqual((result['accepted'], result['failed']), (1, 1))
        self.send.side_effect = None
        retry = await (await self.post()).json()
        self.assertNotIn('duplicate', retry)
        self.assertEqual(retry['accepted'], 1)
        self.assertEqual([call.args[0] for call in self.send.call_args_list].count('device-a'), 1)
        self.assertEqual(self.send.call_args.args[0], 'device-b')
        self.assertTrue((await (await self.post()).json())['duplicate'])
        self.assertEqual(self.send.call_count, 3)

    async def test_missing_user_and_missing_tokens(self):
        self.pool.row = None
        self.assertEqual((await self.post()).status, 404)
        self.pool.row = {'device_token': None}
        self.tokens.return_value = None
        self.assertEqual((await self.post()).status, 409)
        self.send.assert_not_called()

    async def test_username_resolves_recipient_in_target_organization(self):
        self.pool.row = {'id': 2, 'device_token': '[]'}
        payload = dict(self.payload, username='George')
        del payload['user_id']
        self.assertEqual((await self.post(payload)).status, 200)
        query, params = self.pool.queries[0]
        self.assertIn('username = %s AND organization_id = %s', query)
        self.assertEqual(params, ('George', 1))
        self.assertEqual(self.send.call_args.args[-2:], (2, 1))

    async def test_failure_not_reported_as_success_and_can_be_retested(self):
        self.send.return_value = 'error'
        response = await self.post()
        self.assertEqual(response.status, 502)
        self.assertEqual((await response.json())['accepted'], 0)
        self.send.return_value = 'ok'
        self.assertEqual((await self.post()).status, 200)

    async def test_all_devices_and_malformed_entries(self):
        self.tokens.return_value = [None, {}, {'token': ''}, {'token': 'device-a'},
                                    {'token': 'device-a'}, {'token': 'device-b'}]
        self.send.side_effect = lambda token, *args: 'ok' if token == 'device-a' else 'unregistered'
        result = await (await self.post()).json()
        self.assertEqual((result['accepted'], result['unregistered'], result['failed']), (1, 1, 3))
        self.assertEqual(self.send.call_count, 2)
        # The unregistered token is pruned; other entries are left as stored.
        query, params = self.pool.queries[-1]
        self.assertIn('UPDATE clients SET device_token', query)
        self.assertEqual(json.loads(params[0]), [None, {}, {'token': ''}, {'token': 'device-a'}, {'token': 'device-a'}])
        self.assertEqual(params[1:], (2, 1))

    async def test_database_exception_and_concurrent_duplicate(self):
        self.pool.failure = True
        self.assertEqual((await self.post()).status, 500)
        self.pool.failure = False
        started = asyncio.Event()
        release = asyncio.Event()
        async def blocked_send(*args):
            started.set()
            await release.wait()
            return 'ok'
        with patch('pbx_notifications.asyncio.to_thread', blocked_send):
            first = asyncio.create_task(self.post())
            await started.wait()
            self.assertEqual((await self.post()).status, 409)
            release.set()
            self.assertEqual((await first).status, 200)


class WebsocketCallTests(unittest.IsolatedAsyncioTestCase):
    async def request(self, sender_org=0, target_org=1, valid_session=True, session=None, data=None):
        import secrets
        handlers = [n for n in ast.parse(Path('main.py').read_text()).body
                    if isinstance(n, ast.AsyncFunctionDef) and n.name in {'ws_handler', 'send_call_notification'}]
        pool = Pool()
        pool.row = {'id': 2, 'device_token': '[]'}
        send = Mock(return_value='ok')
        dispatcher = CallNotificationDispatcher(pool, Mock(return_value=[{'token': 'device'}]), send,
                                                logging.getLogger('test-ws'))
        tasks = set()
        namespace = dict(json=json, secrets=secrets, asyncio=asyncio, connected_clients={},
                         call_notification_tasks=tasks,
                         check_user=AsyncMock(return_value={'id': 1, 'username': 'Console', 'organization_id': sender_org}),
                         call_notification_dispatcher=dispatcher,
                         notification_logger=logging.getLogger('test-ws'),
                         websockets=SimpleNamespace(ConnectionClosed=RuntimeError))
        exec(compile(ast.Module(body=handlers, type_ignores=[]), 'main.py', 'exec'), namespace)
        class Socket:
            remote_address = ('127.0.0.1', 1234)
            request_headers = {}
            def __init__(self):
                self.sent = []
            async def send(self, message):
                self.sent.append(json.loads(message))
            async def messages(self):
                yield json.dumps({'event': 'Register', 'username': 'Console', 'token': 'shared-password'})
                token = self.sent[0]['data'] if valid_session else (session or 'incorrect')
                yield json.dumps({'event': 'call_notification', 'username': 'George', 'organization_id': target_org,
                                  'data': dict({'session_token': token, 'call_id': 'call-123'}, **(data or {}))})
            def __aiter__(self):
                return self.messages()
        socket = Socket()
        await namespace['ws_handler'](socket)
        await asyncio.gather(*tasks)
        return socket.sent[-1], pool, send

    async def test_shared_sender_targets_separate_recipient(self):
        reply, pool, send = await self.request()
        self.assertEqual(reply['event'], 'call_notification_success')
        self.assertEqual(pool.queries[0][1], ('George', 1))
        self.assertEqual(send.call_args.args[-2:], (2, 1))

    async def test_session_and_tenant_validation_precede_token_lookup(self):
        for options, error in [({'valid_session': False}, 'invalid_session'),
                               ({'valid_session': False, 'session': 'tökén'}, 'invalid_session'),
                               ({'sender_org': 2}, 'recipient_not_authorized'),
                               ({'sender_org': 1}, 'recipient_not_authorized')]:
            reply, pool, send = await self.request(**options)
            self.assertEqual(reply['error'], error)
            self.assertEqual(pool.queries, [])
            send.assert_not_called()

    async def test_string_organization_and_null_caller_are_accepted(self):
        reply, pool, send = await self.request(target_org='1', data={'caller_name': None, 'caller_number': '100'})
        self.assertEqual((reply['event'], reply['call_id']), ('call_notification_success', 'call-123'))
        self.assertEqual(pool.queries[0][1], ('George', 1))
        self.assertEqual(send.call_args.args[2], '100')

    async def test_invalid_organization_reply_carries_call_id(self):
        reply, pool, send = await self.request(target_org='abc')
        self.assertEqual((reply['error'], reply['call_id']), ('invalid_organization_id', 'call-123'))
        send.assert_not_called()


class RoomParticipantTests(unittest.IsolatedAsyncioTestCase):
    async def test_numeric_participant_ids_are_scoped_to_the_organization(self):
        import aiomysql
        names = {'get_user_id_in_organization', 'resolve_user_ids'}
        funcs = [n for n in ast.parse(Path('main.py').read_text()).body
                 if isinstance(n, ast.AsyncFunctionDef) and n.name in names]
        namespace = dict(aiomysql=aiomysql)
        exec(compile(ast.Module(body=funcs, type_ignores=[]), 'main.py', 'exec'), namespace)
        pool = Pool()
        pool.row = None
        self.assertEqual(await namespace['resolve_user_ids'](pool, ['99'], 1, 5), [1])
        self.assertIn('id = %s AND organization_id = %s', pool.queries[0][0])
        self.assertEqual(pool.queries[0][1], (99, 5))
        pool.row = {'id': 7}
        self.assertEqual(sorted(await namespace['resolve_user_ids'](pool, ['7'], 1, 5)), [1, 7])


class FirebaseCallPayloadTests(unittest.TestCase):
    def test_call_options_encode_with_real_sdk(self):
        import hashlib
        from datetime import datetime, timedelta
        from firebase_admin import exceptions, messaging
        tree = ast.parse(Path('main.py').read_text())
        funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name in {'device_token_fingerprint', 'send_push_notification'}]
        namespace = dict(hashlib=hashlib, datetime=datetime, timedelta=timedelta,
                         json=json, messaging=messaging, exceptions=exceptions, config=SimpleNamespace(),
                         notification_logger=logging.getLogger('test-fcm'),
                         firebase_admin=SimpleNamespace(get_app=lambda: SimpleNamespace(project_id='test')))
        exec(compile(ast.Module(body=funcs, type_ignores=[]), 'main.py', 'exec'), namespace)
        with patch.object(messaging, 'send', return_value='test-message') as send:
            self.assertEqual(namespace['send_push_notification']('token', 'Title', 'Body', {'type': 'incoming_call'}), 'ok')
            encoded = json.loads(str(send.call_args.args[0]))
            self.assertEqual(encoded['android']['priority'], 'high')
            self.assertEqual(encoded['android']['ttl'], '30s')
            self.assertEqual(encoded['apns']['headers']['apns-push-type'], 'alert')
            self.assertEqual(encoded['apns']['payload']['aps']['sound'], 'default')
            self.assertEqual(encoded['notification'], {'title': 'Title', 'body': 'Body'})
            self.assertNotIn('alert', encoded['apns']['payload']['aps'])
            namespace['send_push_notification']('token', 'Title', 'Body', {'type': 'notification'})
            encoded = json.loads(str(send.call_args.args[0]))
            self.assertNotIn('android', encoded)
            self.assertNotIn('apns', encoded)

            # Data-only for Android, while iOS keeps its alert on the APNs payload.
            namespace['config'].CALL_PUSH_ANDROID_DATA_ONLY = True
            self.assertEqual(namespace['send_push_notification'](
                'token', 'Title', 'Body', {'type': 'incoming_call', 'call_id': 'c1'}), 'ok')
            encoded = json.loads(str(send.call_args.args[0]))
            self.assertNotIn('notification', encoded)
            self.assertEqual(encoded['data'], {'type': 'incoming_call', 'call_id': 'c1', 'title': 'Title', 'body': 'Body'})
            self.assertEqual(encoded['android']['priority'], 'high')
            self.assertEqual(encoded['android']['ttl'], '30s')
            self.assertEqual(encoded['apns']['payload']['aps']['alert'], {'title': 'Title', 'body': 'Body'})
            self.assertEqual(encoded['apns']['payload']['aps']['sound'], 'default')
            self.assertEqual(encoded['apns']['headers']['apns-push-type'], 'alert')
            # Other notifications are unchanged by the setting.
            namespace['send_push_notification']('token', 'Title', 'Body', {'type': 'notification'})
            encoded = json.loads(str(send.call_args.args[0]))
            self.assertEqual(encoded['notification'], {'title': 'Title', 'body': 'Body'})
            self.assertNotIn('android', encoded)


if __name__ == '__main__':
    unittest.main()
