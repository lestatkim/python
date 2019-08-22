import os
import shutil
from flask import Flask, jsonify, request
from flask_restful import Resource, Api
import flask_cors as cors
from datetime import timedelta
from log import *


# Flask
app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = 'red bones white feet'
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=JWT_EXPIRE_MINUTES)

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

api = Api(app)


class Deviants(Resource):
    @staticmethod
    def post():
        try:
            files_list_from_request = request.json["files"]
        except KeyError as err:
            log.exception(err)
            return {"error": "KeyError: {}".format(str(err))}, 400
        existing_files_list = os.listdir(TEMP_PHOTO_DIR)

        repeatable_files = list(set(files_list_from_request) & set(existing_files_list))
        if not os.path.isdir(SHARED_FILE_PATH):
            try:
                os.makedirs(SHARED_FILE_PATH)
                log.info('Folder "' + SHARED_FILE_PATH + '"created')
            except OSError as err:
                log.exception(err)
                return {"error": "OSError: {}".format(str(err))}, 500
        if repeatable_files:
            for file in repeatable_files:
                try:
                    shutil.move(TEMP_PHOTO_DIR + '/' + file, SHARED_FILE_PATH + '/' + file)
                except OSError as err:
                    log.exception(err)
                    return {"error": "OSError: {}".format(str(err))}, 500
                log.info('File "' + file + '"moved to ' + SHARED_FILE_PATH)

        return jsonify({"success": True})

    @staticmethod
    def get():
        if not os.path.isdir(SHARED_FILE_PATH):
            try:
                os.makedirs(SHARED_FILE_PATH)
                log.info('Folder "' + SHARED_FILE_PATH + '"created')
            except OSError as err:
                log.exception(err)
                return {"error": "OSError: {}".format(str(err))}, 500
        try:
            files = os.listdir(SHARED_FILE_PATH)
        except OSError as err:
            log.exception(err)
            return {"error": "OSError: {}".format(str(err))}, 500

        return jsonify({"files": files})

    @staticmethod
    def delete():
        try:
            filename = request.args.get("filename")
        except KeyError as err:
            log.exception(err)
            return {"error": "KeyError: {}".format(str(err))}, 400
        full_name = SHARED_FILE_PATH + '/' + filename
        if os.path.isfile(full_name):
            try:
                os.remove(full_name)
                log.info('file ' + filename + ' deleted')
            except OSError as err:
                log.exception(err)
                return {"error": "Could not remove file {}: {}".format(full_name, str(err))}, 500
        else:
            log.info('File ' + filename + ' does not exist, cannot delete')
        return jsonify({"success": True})


class Upload(Resource):
    @staticmethod
    def post():
        if 'file' not in request.files:
            return {"error": "no file attached"}, 400
        file = request.files['file']
        if file.filename == '':
            return {"error": "no filename"}, 400
        if not os.path.isdir(TEMP_PHOTO_DIR):
            try:
                os.makedirs(TEMP_PHOTO_DIR)
                log.info('Folder "' + TEMP_PHOTO_DIR + '"created')
            except OSError as err:
                log.exception(err)
                return {"error": "OSError: {}".format(str(err))}, 500

        image_list = os.listdir(TEMP_PHOTO_DIR)
        int_name_image_list = list()
        for image in image_list:
            image_name_without_extension = image.split('.')[0]
            try:
                int_image_name = int(image_name_without_extension)
                int_name_image_list.append(int_image_name)
            except ValueError:
                pass
        filename_numeric = max(int_name_image_list) + 1 if int_name_image_list else 0
        while os.path.isfile(os.path.join(SHARED_FILE_PATH, str(filename_numeric) + '.jpg')):
            filename_numeric = filename_numeric + 1
        filename = str(filename_numeric)

        full_path = os.path.join(TEMP_PHOTO_DIR, filename + '.jpg')
        try:
            file.save(full_path)
            if os.path.isfile(full_path):
                log.info('File "%s" uploaded to temp folder' % filename)
            else:
                raise OSError(2, 'No such file or directory', full_path)
        except OSError as err:
            log.exception(err)
            return {"error": "Could not save file {}: {}".format(full_path, str(err))}, 500
        return jsonify({"filename": filename + '.jpg'})


class Regenerate(Resource):
    @staticmethod
    def post():
        os.system('source /opt/projects/blgenerator/venv/bin/activate')
        os.system('python /opt/projects/blgenerator/generator.py')
        os.system('deactivate')


api.add_resource(Deviants, '/api/deviants')
api.add_resource(Upload, '/api/upload')
api.add_resource(Regenerate, '/api/regenerate')

if __name__ == "__main__":
    try:
        app.run(debug=DEBUG)
    except Exception as e:
        log.exception(e)
