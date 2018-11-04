#!/usr/bin/env python3

from flask import Flask, jsonify, abort, request
import hashlib
import requests
import logging

from cr import ConsistentHashRing

COPIES = 3
REPLICAS = 100
NODES = 5
CHUNK_SIZE = 4096*1024

app = Flask(__name__)

ring = ConsistentHashRing(REPLICAS)

for i in range(NODES):
    ring["node%d" % i] = "flatdb-%d.flatdb.storage" % i


@app.route('/healthy', methods=['GET'])
def healthy():
    return jsonify({'healthy': 'yes'})


@app.route('/ready', methods=['GET'])
def ready():
    return jsonify({'ready': 'yes'})


@app.route('/api/v1.0/objects/<name>', methods=['PUT'])
def put(name):
    data = b""
    m = hashlib.sha1()

    while True:
        chunk = request.stream.read(CHUNK_SIZE)
        if len(chunk) == 0:
            break

        m.update(chunk)
        data += chunk

    return jsonify({'hash': m.hexdigest()}), min(put_in_node(name, data))


def put_in_node(name, data):
    ret = []
    for copy in range(COPIES):
        node = ring[name + str(copy)]
        logging.warning(f"{name} {copy}: {node}")
        try:
            response = requests.put(f"http://{node}:5001/putblob",
                                    data=data,
                                    headers={'Content-Type': 'application/octet-stream'},
                                    params={'key': name},
                                    )
            ret.append(response.status_code)
        except requests.exceptions.ConnectionError:
            ret.append(requests.status_codes.codes['service_unavailable'])
            logging.exception("PUT FAILED")

    return ret


@app.route('/api/v1.0/objects/<name>', methods=['GET'])
def get_object(name):
    ret = []
    for copy in range(COPIES):
        data, code = get_from_node(name, copy)
        ret.append(code)
        if code < 300:
            break

    return data, min(ret)


def get_from_node(name, copy):
    data = b''
    node = ring[name + str(copy)]
    try:
        response = requests.get(f"http://{node}:5001/getblob",
                                {'key': name},
                                headers={'Content-Type': 'application/octet-stream'},
                                )
        data = response.text

    except requests.exceptions.ConnectionError:
        logging.exception("GET FAILED")
        return data, requests.status_codes.codes['service_unavailable']
    return data, response.status_code


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
