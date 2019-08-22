import unittest
from datamart import *
import json


class ApiDeviationTestCase(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        os.environ['DATAMART_PG_CONN_STRING'] = PG_TEST_CONN_STRING

        # Delete test data, objects, camera, users, relation's, superuser
        pg_exec("""truncate table...""", False)

        # Insert test data, objects, camera, users, relation's, superuser
        pg_exec("insert into...", False)

    def tearDown(self):
        # Delete test data
        pg_exec("""truncate table...""", False)
        del os.environ['DATAMART_PG_CONN_STRING']

    def _get_jwt_auth_headers(self):
        resource = '/api/login'
        response = self.app.post(
            resource,
            data=json.dumps({"username": "username", "password": "password"}),
            content_type='application/json'
        )
        self.assertEqual(200, response.status_code, resource)
        res = json.loads(response.data.decode('utf-8'))
        token = res['token']
        headers = {
            "Authorization": "Bearer %s" % token,
            "content-type": 'application/json'
        }
        return headers

    def test_deviation_endpoint(self):
        url = '/api/deviation'
        headers = self._get_jwt_auth_headers()
        request_data = {
            "object_id": 2,
            "camera_id": 20,
            "start": '20190101',
            "end": '20190131'
        }
        valid_response_json = {'idx': ['2019-01-18'],
                               'name': ['18'],
                               'bad': ['0.00'],
                               'good': ['0.00']}

        # Auth OK request OK -> 200 OK
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )

        # Testing endpoint Deviation
        self.assertEqual(200, response.status_code)
        self.assertEqual(valid_response_json, response.json['result'])

    def test_bad_deviation(self):
        url = '/api/bad_deviation'
        headers = self._get_jwt_auth_headers()
        request_data = {
            "object_id": 2,
            "start": "2018-12-14",
            "end": "2019-01-18"
        }
        valid_response_json = {
            "idx": ['2018-12-14', '2019-01-18'],
            "values": [1, 1]
        }
        # Auth OK request OK -> 200 OK
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )

        # Testing endpoint BadDeviation
        self.assertEqual(200, response.status_code)
        self.assertEqual(valid_response_json, response.json['result'])


class ApiDynamicOfEmotionsTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        os.environ['DATAMART_PG_CONN_STRING'] = PG_TEST_CONN_STRING

        # Delete test data, objects, camera, users, relation's, superuser
        pg_exec("""truncate table...""", False)

        # Insert test data, objects, camera, users, relation's, superuser
        pg_exec("insert into...", False)

    def tearDown(self):
        # Delete test data
        pg_exec("""truncate table...""", False)
        del os.environ['DATAMART_PG_CONN_STRING']

    def _get_jwt_auth_headers(self):
        resource = '/api/login'
        response = self.app.post(
            resource,
            data=json.dumps({"username": "username", "password": "password"}),
            content_type='application/json'
        )
        self.assertEqual(200, response.status_code, resource)
        res = json.loads(response.data.decode('utf-8'))
        token = res['token']
        headers = {
            "Authorization": "Bearer %s" % token,
            "content-type": 'application/json'
        }
        return headers

    def test_sql_request(self):
        query = queries['dynamic_of_emotions']
        _, res = pg_exec(query.format(object_id=2, start='2019-01-01', end='2019-02-01'))
        self.assertEqual([('2019-01-01', 1, 0, 1), ('2019-02-01', 0, 0, 1)], res)

    def test_dynamic_of_emotions_endpoint(self):
        url = '/api/dynamic_of_emotions'
        headers = self._get_jwt_auth_headers()
        request_data = {
            "object_id": 2,
            "start": "2019-01-01",
            "end": "2019-02-01"
        }
        valid_response_json = {
            "idx": ["2019-01-01", "2019-02-01"],
            "good": [1, 0],
            "same": [0, 0],
            "bad": [1, 1]
        }

        # Auth OK request OK -> 200 OK
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )
        # Testing endpoint BadDeviation
        self.assertEqual(200, response.status_code)
        self.assertEqual(valid_response_json, response.json['result'])

    def test_schedule(self):
        url = '/api/schedule'
        headers = self._get_jwt_auth_headers()
        request_data = {
            "object_id": 2,
            "start": "2019-02-01",
            "end": "2019-02-02",
            "data": [
                {
                    "value": False,
                    "name": "asdfghj",
                    "start": "20190201",
                    "good": "0.03",
                    "bad": "0.16",
                    "date": "2019-02-01",
                    "comment": ""
                },
                {
                    "value": False,
                    "name": "zSDXFC",
                    "start": "20190202",
                    "good": "0.02",
                    "bad": "0.23",
                    "date": "2019-02-02",
                    "comment": "asedaaaa"
                }
            ]
        }
        for item in request_data["data"]:
            pg_exec(
                queries["schedule"]["update_insert_if_exists"].format(
                    object_id=request_data["object_id"],
                    date=item["date"],
                    name=item["name"],
                    comment=item["comment"]
                ), False
            )
        valid_response_json = {"success": True}
        # Auth OK request OK -> 200 OK
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )

        # Testing endpoint BadDeviation
        self.assertEqual(200, response.status_code)
        self.assertEqual(valid_response_json, response.json)


class LoyaltyTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        os.environ['DATAMART_PG_CONN_STRING'] = PG_TEST_CONN_STRING

        # Delete test data, objects, camera, users, relation's, superuser
        pg_exec("""truncate table...""", False)

        # Insert test data, objects, camera, users, relation's, superuser
        pg_exec("insert into...", False)

    def tearDown(self):
        # Delete test data
        pg_exec("""truncate table...""", False)
        del os.environ['DATAMART_PG_CONN_STRING']

    def _get_jwt_auth_headers(self):
        resource = '/api/login'
        response = self.app.post(
            resource,
            data=json.dumps({"username": "username", "password": "password"}),
            content_type='application/json'
        )
        self.assertEqual(200, response.status_code, resource)
        res = json.loads(response.data.decode('utf-8'))
        token = res['token']
        headers = {
            "Authorization": "Bearer %s" % token,
            "content-type": 'application/json'
        }
        return headers

    def test_sql_request(self):
        query = queries['loyalty']['loyalty']
        _, res = pg_exec(query.format(object_id=2))
        self.assertEqual([(1, 1, 3, 2)], res)

    def test_endpoint(self):
        request_data = {
            "object_id": 2,
            "start": "2018-12-14",
            "end": "2019-01-18",
            "camera_id": 20
        }
        valid_response = {
            'more_than_2_times_in_2_weeks': [1],
            'more_than_2_times_a_week': [1],
            'more_than_2_times_a_month': [3],
            'less_than_2_days_per_month': [2],
        }

        headers = self._get_jwt_auth_headers()
        url = '/api/loyalty'
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )
        self.assertEqual(200, response.status_code)
        self.assertTrue(response.json['success'])

        request_data = {
            "object_id": 2
        }

        url = '/api/fast_loyalty'
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )
        self.assertEqual(200, response.status_code)
        self.assertTrue(response.json['success'])
        self.assertEqual(valid_response, response.json['result'])


class UniqueTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        os.environ['DATAMART_PG_CONN_STRING'] = PG_TEST_CONN_STRING

        # Delete test data, objects, camera, users, relation's, superuser
        pg_exec("""truncate table...""", False)

        # Insert test data, objects, camera, users, relation's, superuser
        pg_exec("insert into...", False)

    def tearDown(self):
        # Delete test data
        pg_exec("""truncate table...""", False)
        del os.environ['DATAMART_PG_CONN_STRING']

    def _get_jwt_auth_headers(self):
        resource = '/api/login'
        response = self.app.post(
            resource,
            data=json.dumps({"username": "username", "password": "password"}),
            content_type='application/json'
        )
        self.assertEqual(200, response.status_code, resource)
        res = json.loads(response.data.decode('utf-8'))
        token = res['token']
        headers = {
            "Authorization": "Bearer %s" % token,
            "content-type": 'application/json'
        }
        return headers

    def test_unique_endpoint(self):
        url = '/api/unique'
        headers = self._get_jwt_auth_headers()
        request_data = {
            "object_id": 2,
            "camera_id": 20,
            "start": "2018-12-14",
            "end": "2019-01-18"
        }
        valid_response_json = {
            "weekly": {
                "values": [1, 1],
                "idx": ['friday', 'saturday']
            },
            "daily": {
                "values": [1, 1],
                "idx": [15, 17]
            },
            "for_period": {
                "values": [1, 1, 1],
                "idx": ['2018-12-14', '2018-12-15', '2019-01-18']
            }
        }

        # Auth OK request OK -> 200 OK
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )

        # Testing endpoint Deviation
        self.assertEqual(200, response.status_code)
        self.assertEqual(valid_response_json, response.json['result'])

    def test_sql_request_daily(self):
        query = queries['unique']['daily'].format(
            object_id=2,
            camera_id=20,
            start='2018-12-14',
            end='2019-01-18'
        )
        valid_result = [(15, 1), (17, 1)]
        _, res = pg_exec(query)
        self.assertEqual(valid_result, res)

    def test_sql_request_weekly(self):
        query = queries['unique']['weekly'].format(
            object_id=2,
            camera_id=20,
            start='2018-12-14',
            end='2019-01-18'
        )
        valid_result = [('friday', 1), ('saturday', 1)]
        _, res = pg_exec(query)
        self.assertEqual(valid_result, res)

    def test_sql_request_for_period(self):
        query = queries['unique']['period'].format(
            object_id=2,
            camera_id=20,
            start='2018-12-14',
            end='2019-01-18'
        )
        valid_result = [('2018-12-14', 1), ('2018-12-15', 1), ('2019-01-18', 1)]
        _, res = pg_exec(query)
        self.assertEqual(valid_result, res)


class VisitorTimeTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        os.environ['DATAMART_PG_CONN_STRING'] = PG_TEST_CONN_STRING

        # Delete test data, objects, camera, users, relation's, superuser
        pg_exec("""truncate table...""", False)

        # Insert test data, objects, camera, users, relation's, superuser
        pg_exec("insert into...", False)

    def tearDown(self):
        # Delete test data
        pg_exec("""truncate table...""", False)
        del os.environ['DATAMART_PG_CONN_STRING']

    def _get_jwt_auth_headers(self):
        resource = '/api/login'
        response = self.app.post(
            resource,
            data=json.dumps({"username": "username", "password": "password"}),
            content_type='application/json'
        )
        self.assertEqual(200, response.status_code, resource)
        res = json.loads(response.data.decode('utf-8'))
        token = res['token']
        headers = {
            "Authorization": "Bearer %s" % token,
            "content-type": 'application/json'
        }
        return headers

    def test_sql_request(self):
        query = queries['visitor_time']['visitor_time'].format(
            object_id=2,
            camera_id=20,
            start='2018-12-14',
            end='2019-01-18'
        )
        valid_result = [
            (3, '0-15'),
            (2, '15-30'),
            (1, '30-45'),
            (1, '75-120')
        ]
        _, res = pg_exec(query)
        self.assertEqual(valid_result, res)

    def test_endpoint(self):
        request_data = {
            "object_id": 2,
            "camera_id": 20,
            "start": '2018-12-14',
            "end": '2019-01-18'
        }
        valid_response = {
            "values": [3, 2, 1, 1],
            "idx": ["0-15", "15-30", "30-45", "75-120"]
        }

        headers = self._get_jwt_auth_headers()
        url = '/api/visitortime'
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )
        self.assertEqual(200, response.status_code)
        self.assertTrue(response.json['success'])
        self.assertEqual(valid_response, response.json['result'])


class TypicalDayTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        os.environ['DATAMART_PG_CONN_STRING'] = PG_TEST_CONN_STRING

        # Delete test data, objects, camera, users, relation's, superuser
        pg_exec("""truncate table...""", False)

        # Insert test data, objects, camera, users, relation's, superuser
        pg_exec("insert into...", False)

    def tearDown(self):
        # Delete test data
        pg_exec("""truncate table...""", False)
        del os.environ['DATAMART_PG_CONN_STRING']

    def _get_jwt_auth_headers(self):
        resource = '/api/login'
        response = self.app.post(
            resource,
            data=json.dumps({"username": "username", "password": "password"}),
            content_type='application/json'
        )
        self.assertEqual(200, response.status_code, resource)
        res = json.loads(response.data.decode('utf-8'))
        token = res['token']
        headers = {
            "Authorization": "Bearer %s" % token,
            "content-type": 'application/json'
        }
        return headers

    def test_sql_request(self):
        query = queries["typical_day"]["wide_select"]
        valid_result = [
            ('tuesday', 11, '0.5', '1.0'),
            ('tuesday', 12, '1.0', '1.0'),
            ('wednesday', 18, '0.5', '2.0'),
            ('wednesday', 20, '0.5', '1.5')
        ]
        _, res = pg_exec(query.format(object_id=2, camera_id=20))
        self.assertEqual(valid_result, res)

    def test_endpoint(self):
        request_data = {
            "object_id": 2,
            "camera_id": 20
        }
        valid_res_obj = {
            "day_of_week": ["tuesday", "tuesday", "wednesday", "wednesday"],
            "hour": [11, 12, 18, 20],
            "good": ['0.5', '1.0', '0.5', '0.5'],
            "bad": ['1.0', '1.0', '2.0', '1.5']
        }

        headers = self._get_jwt_auth_headers()
        url = '/api/typical_day'
        response = self.app.post(
            url,
            data=json.dumps(request_data),
            content_type='application/json',
            headers=headers
        )
        self.assertEqual(200, response.status_code)
        self.assertTrue(response.json['success'])
        self.assertEqual(valid_res_obj, response.json['result'])


class PgExecDecimalFloatDatetimeTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        os.environ['DATAMART_PG_CONN_STRING'] = PG_TEST_CONN_STRING

    def tearDown(self):
        del os.environ['DATAMART_PG_CONN_STRING']

    def test_sql_request(self):
        valid_result = [('3.14159', '0.001', '2019-01-01', '2019-01-01T12:00:00')]
        _, res = pg_exec("""
            select 3.14159::decimal, 0.001::float(32)
                , '20190101'::date
                , '20190101 12:00:00'::timestamp without time zone
            ;
        """)
        self.assertEqual(valid_result, res)


if __name__ == '__main__':
    unittest.main()
