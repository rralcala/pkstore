#!/usr/bin/env python3

from flask import Flask, jsonify, abort, request
import hashlib
import requests

from cr import ConsistentHashRing

app = Flask(__name__)

ring = ConsistentHashRing(100)

for i in range(0, 10):
    ring["node%d" % i] = "flatdb-%d" % i


@app.route('/healthy', methods=['GET'])
def healthy():
    return jsonify({'healthy': 'yes'})


@app.route('/ready', methods=['GET'])
def ready():
    return jsonify({'ready': 'yes'})


@app.route('/api/v1.0/objects/<name>', methods=['PUT'])
def update_task(name):

    chunk_size = 4096*1024
    data = b""
    m = hashlib.sha1()

    while True:
        chunk = request.stream.read(chunk_size)
        if len(chunk) == 0:
            break

        m.update(chunk)
        data += chunk

    node = ring[name]
    try:
        response = requests.put(f"http://{node}:5001/putblob",
                                data=data,
                                headers={'content-type': 'application/octet-stream'},
                                params={'key': name},
                                )
    except requests.exceptions.ConnectionError:
        return '', requests.status_codes.codes['service_unavailable']

    return jsonify({'hash': m.hexdigest()}), response.status_code


@app.route('/api/v1.0/objects/<name>', methods=['GET'])
def get_task(name):
    node = ring[name]
    try:
        response = requests.get(f"http://{node}:5001/getblob",
                                {'key': name},
                                headers={'content-type': 'application/octet-stream'},
                                )
    except requests.exceptions.ConnectionError:
        return '', requests.status_codes.codes['service_unavailable']
    if response.status_code >= 300:
        return '', response.status_code
    else:
        return response.text, response.status_code


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
