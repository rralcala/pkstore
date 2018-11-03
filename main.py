#!/usr/bin/env python3
from flask import Flask, jsonify, abort, request
import hashlib
import json
app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]


@app.route('/api/v1.0/objects', methods=['GET'])
def get_tasks():
    return jsonify({'object': tasks})


@app.route('/healthy', methods=['GET'])
def healthy():
    return jsonify({'healthy': 'yes'})


@app.route('/ready', methods=['GET'])
def ready():
    return jsonify({'ready': 'yes'})


@app.route('/api/v1.0/objects', methods=['PUT'])
def create_task():

    chunk_size = 4096*1024

    m = hashlib.sha1()
    while True:
        chunk = request.stream.read(chunk_size)
        if len(chunk) == 0:
            break

        m.update(chunk)

    task = {
        'id': tasks[-1]['id'] + 1,
        'description': m.hexdigest(),
        'done': False
    }
    tasks.append(task)
    return jsonify({'task': task}), 201


@app.route('/api/v1.0/objects/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    task = [task for task in tasks if task['id'] == task_id]
    print(type(request.json['description']))
    if len(task) == 0:
        abort(404)
    if not request.json:
        abort(400)
    if 'description' in request.json and type(request.json['description']) is not str:
        abort(400)
    if 'done' in request.json and type(request.json['done']) is not bool:
        abort(400)
    task[0]['description'] = request.json.get('description', task[0]['description'])
    task[0]['done'] = request.json.get('done', task[0]['done'])
    return jsonify({'task': task[0]})


@app.route('/api/v1.0/objects/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task = [task for task in tasks if task['id'] == task_id]
    if len(task) == 0:
        abort(404)
    return jsonify({'task': task[0]})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
