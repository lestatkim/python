import psycopg2
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from flask_jwt_extended import JWTManager, create_access_token
from query import queries
import flask_cors as cors
from log import log
import re
import os
from decimal import Decimal
from datetime import date
from config import *


# Flask
app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = 'red bones white feet'
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=JWT_EXPIRE_MINUTES)

api = Api(app)
jwt = JWTManager(app)
cors.CORS(
    app, origins="*", allow_headers=[
        "X-Real-Ip",
        "Content-Type",
        "Authorization",
        "Access-Control-Allow-Credentials",
        "X-Custom-Header",
        "Cache-Control",
        "x-Requested-With"
    ],
    supports_credentials=True)


def pg_exec(query, ret=True):
    log.debug('SQL QUERY: ' + re.sub(' +', ' ', query).replace('\n', ' '))
    st = datetime.now()
    result = True
    result_list = []
    con_str = os.environ.get('DATAMART_PG_CONN_STRING')
    if con_str is None:
        con_str = PG_CONN_STRING
    with psycopg2.connect(con_str) as con:
        cur = con.cursor()
        cur.execute(query)
        if ret:
            res = cur.fetchall()
            for row in res:
                new_row = []
                for value in row:
                    if isinstance(value, datetime):
                        value = value.isoformat()
                    elif isinstance(value, date):
                        value = value.isoformat()
                    elif isinstance(value, Decimal):
                        if int(value) == value:
                            value = int(value)
                        else:
                            value = str(value)
                    elif isinstance(value, float):
                        value = str(value)
                    new_row.append(value)
                result_list.append(tuple(new_row))

            column_name = [desc[0] for desc in cur.description]
            con.commit()
            result = column_name, result_list
    log.debug('Query finished in %s' % (datetime.now() - st))
    return result


class Login(Resource):
    @staticmethod
    def post():
        """

        :return: token
        """
        try:
            data = request.json
        except Exception as e:
            return jsonify({"success": False, "error": repr(e)})

        _, res = pg_exec(queries["user"].format(username=data['username'], password=data['password']))

        if not res:
            return {"success": False, "error": "Incorrect username or password"}, 401
        else:
            objects_list = tuple({object_id[0] for object_id in res})
            access_token = create_access_token(
                identity={"username": data['username'], "objects": objects_list, "is_superuser": res[0][4]})
            objects = list()
            for obj in objects_list:
                objects.append(
                    {
                        "object": (obj,),
                        "cameras": tuple({i[1] for i in res if i[0] == obj}),
                        "object_name": [i[2] for i in res if i[0] == obj][0],
                        "address": tuple({i[3] for i in res if i[0] == obj})
                    }
                )

            response = {
                "token": access_token,
                "objects": objects
            }
            if res[0][4]:
                response["is_superuser"] = True

        return jsonify(response)


class Unique(Resource):
    @jwt_required
    def post(self):
        """

        :return: daily and weekly unique user list
        """
        try:
            data = request.json
        except Exception as e:
            return jsonify({"success": False, "error": repr(e)})

        try:
            _, daily = pg_exec(queries["unique"]["daily"].format(
                object_id=data["object_id"], camera_id=data["camera_id"], start=data["start"], end=data["end"]
            ))
        except Exception as e:
            return jsonify({"success": False, "error": repr(e)})

        try:
            _, weekly = pg_exec(queries["unique"]["weekly"].format(
                object_id=data["object_id"], camera_id=data["camera_id"], start=data["start"], end=data["end"]
            ))
        except Exception as e:
            return jsonify({"success": False, "error": repr(e)})

        _, for_period = pg_exec(queries["unique"]["period"].format(
            object_id=data["object_id"], camera_id=data["camera_id"], start=data["start"], end=data["end"]
        ))

        return jsonify({
            "success": True,
            "result": {
                "weekly": {"values": [int(i[1]) for i in weekly], "idx": [i[0] for i in weekly]},
                "daily": {"values": [int(i[1]) for i in daily], "idx": [i[0] for i in daily]},
                "for_period": {"values": [int(i[1]) for i in for_period], "idx": [i[0] for i in for_period]}
            }})


class Deviation(Resource):
    @jwt_required
    def post(self):
        data = request.json
        try:
            obj, start, end = data["object_id"], data["start"], data["end"]
        except KeyError as e:
            log.exception(e)
            return {"success": False, "error": 'object_d, start and end required'}, 400

        _, good = pg_exec(queries["deviation"]["by_typical_day"].format(
            object_id=obj, start=start, end=end
        ))

        response = {
            'idx': [],
            'good': [],
            'name': [],
            'bad': [],
        }
        for i in good:
            response['idx'].append(i[0])
            response['good'].append('%.2f' % float(i[1]))
            response['name'].append('%s' % i[0][-2:])
            response['bad'].append('%.2f' % float(i[2]))

        return jsonify({"success": True, "result": response})


class Loyalty(Resource):
    @jwt_required
    @check_object_permissions
    def post(self):
        try:
            data = request.json
        except Exception as e:
            return jsonify({"success": False, "error": repr(e)})

        _, loyalty_values = pg_exec(queries["loyalty"]["loyalty"].format(
            object_id=data["object_id"],
            camera_id=data["camera_id"],
            start=data["start"],
            end=data["end"]
        ))

        _, newbie_values = pg_exec(queries["loyalty"]["newbie"].format(
            start=data["start"],
            end=data["end"]
        ))

        return jsonify({
            "success": True,
            "result": {
                "returns": {
                    "values": list(loyalty_values[0]),
                    "idx": ["more_than_2_times_a_week",
                            "more_than_2_times_in_2_weeks",
                            "more_than_2_times_a_month",
                            "less_than_2_days_per_month"]
                },
                "newbies": {
                    "idx": [datetime.strptime(str(i[0]), "%Y-%m-%d").strftime("%d.%m.%Y") for i in newbie_values],
                    "values": [i[1] for i in newbie_values]
                }}})


class VisitorTime(Resource):
    @jwt_required
    @check_object_permissions
    def post(self):
        try:
            data = request.json
        except Exception as e:
            return jsonify({"success": False, "error": repr(e)})

        if data is None:
            return jsonify({"success": False, "error": "request is None", "data": data})

        _, visitor_time = pg_exec(queries["visitor_time"]["visitor_time"].format(
            object_id=data["object_id"], camera_id=data["camera_id"], start=data["start"], end=data["end"]
        ))

        return jsonify({
            "success": True,
            "result": {
                "values": [i[0] for i in visitor_time],
                "idx": [i[1] for i in visitor_time]
            }
        })


class AvgTimeMonthly(Resource):
    @jwt_required
    @check_object_permissions
    def post(self):
        try:
            data = request.json
        except Exception as e:
            return jsonify({"success": False, "error": repr(e)})

        if data is None:
            return jsonify({"success": False, "error": "request is None", "data": data})

        _, avg_time_monthly = pg_exec(queries["visitor_time"]["avg_time_monthly"].format(
            object_id=data["object_id"], camera_id=data["camera_id"], start=data["start"], end=data["end"]
        ))

        return jsonify({
            "success": True,
            "result": {
                "values": [i[0] for i in avg_time_monthly],
                "idx": [i[1] for i in avg_time_monthly]
            }
        })


class AggressiveFaces(Resource):
    @check_object_permissions
    @jwt_required
    def post(self):
        data = request.json

        # Validate request JSON fields:
        if set(data.keys()) != {"selected_date", "object_id"}:
            return {"success": False, "error": "day and object_id required"}, 400

        _, aggressive_track_id = pg_exec(queries["deviation"]["tracks"].format(
            selected_date=data["selected_date"],
            object_id=data["object_id"]
        ))
        return jsonify({
            "success": True,
            "result": {
                "values": [i[0] for i in aggressive_track_id]
            }
        })


api.add_resource(Login, '/api/login')
api.add_resource(Unique, '/api/unique')
api.add_resource(Loyalty, '/api/loyalty')
api.add_resource(VisitorTime, '/api/visitortime')
api.add_resource(Deviation, '/api/deviation')
api.add_resource(AvgTimeMonthly, '/api/avg_time_monthly')


if __name__ == '__main__':
    app.run(debug=DEBUG)
