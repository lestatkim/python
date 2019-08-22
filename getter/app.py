import psycopg2
from functools import wraps
import os
from datetime import datetime
from flask import Flask, request, jsonify, Response
from flask_restful import Resource, Api
from flask_jwt_extended import JWTManager, create_access_token
import config as c
import flask_cors as cors
from flask_jwt_extended.view_decorators import verify_jwt_in_request
from jwt import exceptions as jwte

# Flask config
app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = 'red bones white feet'
api = Api(app)
jwt = JWTManager(app)
cors.CORS(
    app, origins="*", allow_headers=[
        "X-Real-Ip",
        "Content-Type",
        "Authorization",
        "Access-Control-Allow-Credentials",
        "X-Custom-Header"
    ],
    supports_credentials=True)


def jwt_required(fn):
    """
    FORK
    A decorator to protect a Flask endpoint.

    If you decorate an endpoint with this, it will ensure that the requester
    has a valid access token before allowing the endpoint to be called. This
    does not check the freshness of the access token.

    See also: :func:`~flask_jwt_extended.fresh_jwt_required`
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            verify_jwt_in_request()
        except jwte.PyJWTError as e:
            return {"success": False, "error": e.__repr__()}, 401
        return fn(*args, **kwargs)
    return wrapper


# sql execute
def postgres(typ: str, values = None):
    with psycopg2.connect(c.cnxn_string) as conn:
        cur = conn.cursor()
        if values is not None:
            cur.execute(c.tables[typ] % values)
        else:
            cur.execute(c.tables[typ])
        if 'select' in typ:
            res = cur.fetchall()
            return res[0]
        conn.commit()


# get string timestamp
def dt() -> str:
    return str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


# send telegram message from bot to Eugene Orlov
def curl(camera_id: int, obj: int, pers_local_id: int, came_substract: str, gender_and_age: str) -> None:
    text: str = f'{dt()} POST faces/add OBJ{obj} CAM{camera_id} ' \
                f'face{pers_local_id}, {came_substract} {gender_and_age}'
    curl_str = f'/usr/bin/curl -x http://80.211.41.226:1443 ' \
               f'-L "https://api.telegram.org/bot{c.cam_tokens[camera_id]}/' \
               f'sendMessage?chat_id={c.zheka}&text={text}"'
    os.system(curl_str)
    return None


# get new camera id
class Camera(Resource):
    @jwt_required
    def post(self) -> jsonify:
        data = request.json.get('objects')[0]
        host = data['host']
        model = data['model']
        postgres('camera_insert', f"('{host}', '{model}')")
        camera_id = postgres('camera_select', (host, model))[0]
        return jsonify(success=True, id=camera_id)


# get new object id
class Object(Resource):
    @jwt_required
    def post(self) -> jsonify:
        data = request.json.get('objects')[0]
        city = data['city']
        address = data['address']
        application = data['application']
        postgres('object_insert', f"('{city}', '{address}', '{application}')")
        object_id = postgres('object_select', (city, address, application))[0]
        return jsonify(success=True, id=object_id)


# get token
class Login(Resource):
    @staticmethod
    def post() -> jsonify:
        data = request.json
        username, password = data['username'], data['password']

        valid = postgres('user_select', (username, password))[0]
        if not valid:
            response = jsonify({"msg": "password or login incorrect"})
            return jsonify(response)
        access_token = create_access_token(identity=username)
        return jsonify({"token": access_token})


# upload data to stg.face
class Faces(Resource):
    @jwt_required
    def post(self) -> jsonify:
        values = ''
        data = request.json.get('objects', [])
        for i in data:
            _hsh = i.get('hash', [])
            hsh = str([float(i) for i in _hsh]).replace('[', '(').replace(']', ')')
            vector = hsh.replace('(', '{').replace(')', '}')
            count = i.get('count', 0)
            confidence = i.get('confidence', 0)
            threshold = i.get('threshold', 0)
            sharpness = i.get('sharpness', 0)
            came_in = i.get('came_in', None)
            came_out = i.get('came_out', None)
            ts = i.get('timestamp', '')
            obj = i.get('object_id', '')
            cam = i.get('camera_id', '')
            msg = i.get('message_id', '')
            bs = i.get('best_sharpness', '')
            age = i.get('age', 0)
            gender = i.get('gender', 0)
            neutral, happy, sad, surprise, anger = i['emotions']
            fn = i.get('filename', '')
            pers_local_id = i.get('person_local_id', 0)
            female_value = i.get('female_value', 0)
            yaw = i.get('yaw', 0)
            pitch = i.get('pitch', 0)
            roll = i.get('roll', 0)
            came_substract: str = str(
                datetime.strptime(came_out, '%Y-%m-%d %H:%M:%S.%f') -
                datetime.strptime(came_in, '%Y-%m-%d %H:%M:%S.%f')
            )
            gender_and_age: str = 'MALE ' + str(age) + 'yo' if gender == 1 else 'FEMALE' + str(age) + 'yo'

            # curl(cam, obj, pers_local_id, came_substract, gender_and_age)

            values += f"""('{hsh}'::cube,{count},'{confidence}','{threshold}','{sharpness}',
                to_timestamp('{came_in}','yyyy-mm-dd hh24:mi:ss:us'),
                to_timestamp('{came_out}', 'yyyy-mm-dd hh24:mi:ss:us'),
                to_timestamp('{ts}', 'yyyy-mm-dd hh24:mi:ss:us'),
                {obj}, {cam}, '{msg}', '{bs}', '{vector}',
                {age}, {gender}, '{neutral}', '{happy}', '{sad}', '{surprise}', '{anger}', '{fn}', 
                '{female_value}', '{yaw}', '{pitch}', '{roll}'
                ),\n"""

        postgres('face_insert', values.rstrip()[:-1])
        req, obj = postgres('selftest_select')
        postgres('selftest_update', (req + 1, obj + len(data)))
        return jsonify(success=True)


class Selftest(Resource):
    @staticmethod
    def get() -> jsonify:
        req, obj = postgres('selftest_select')
        return Response(f"requests: {req}\nobjects: {obj}\n")


# route's
api.add_resource(Camera, '/camera/add')
api.add_resource(Object, '/object/add')
api.add_resource(Faces, '/faces/add')
api.add_resource(Login, '/login')
api.add_resource(Selftest, '/selftest')


if __name__ == '__main__':
    app.run()
