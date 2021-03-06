"""
    This Flask web app provides a very simple dashboard to visualize the statistics sent by the spark app.
    The web app is listening on port 5000.
    All apps are designed to be run in Docker containers.

    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin

"""


from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

@app.route('/updateData', methods=['POST'])
def updateData1():
    data1= request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data1', json.dumps(data1))
    return jsonify({'msg': 'success'})
def updateData2():
    data2= request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data2', json.dumps(data2))
    return jsonify({'msg': 'success'})

@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data1 = r.get('data1')
    try:
        data1 = json.loads(data1)
    except TypeError:
        return "waiting for data..."
    try:
        py_index = data1['language'].index('Python')
        py_count = data1['count'][py_index]
        py_avgstar=data1['average_stars'][py_index]
    except ValueError:
        py_count = 0
        py_avgstar=0
    try:
        java_index = data1['language'].index('Java')
        java_count=data1['count'][java_index]
        java_avgstar=data1['average_stars'][java_index]
    except ValueError:
        java_count = 0
        java_avgstar=0
    try:
        c_index = data1['language'].index('C')
        c_count=data1['count'][c_index]
        c_avgstar=data1['average_stars'][c_index]
    except ValueError:
        c_count = 0
        c_avgstar=0
    x = [1, 2, 3]
    height = [py_avgstar, java_avgstar,c_avgstar]
    tick_label = ['Python', 'Java', 'C']
    plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:orange', 'tab:blue','tab:red'])
    plt.ylabel('Average number of stars')
    plt.xlabel('PL')

    plt.savefig('/webapp/static/images/chart.png')
    return render_template('index.html', url='/static/images/chart.png', py_count=py_count, java_count=java_count, c_count=c_count)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
