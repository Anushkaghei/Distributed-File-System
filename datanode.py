import os
import json
import threading
from flask import Flask, request, jsonify, send_file, make_response
import requests
import shutil
import time
import config_param as cp

app = Flask(__name__)

class DataNode:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host
        self.port = port

# Define the directory for data blocks
data_blocks_dir = cp.data_blocks_dir

# Replica factor
replication_factor = 3

def register_with_namenode(name_node_url, host, port):
    data = {'host': host, 'port': port}
    print("registering with namenode: ", name_node_url)
    print("sending request to the namenode-data: ", data)
    response = requests.post(f'{name_node_url}/register', json=data)

    if response.status_code == 200:
        return response.json()['data_node']
    else:
        return None
    
def send_ping_acknowledgment_to_name_node(name_node_url, data_node_id):
    acknowledgment_data = {
        'data_node_id': data_node_id,
        'status': 'active',
    }

    try:
        response = requests.post(f'{name_node_url}/acknowledge_ping', json=acknowledgment_data)

        if response.status_code == 200:
            print(f"Acknowledgment sent to NameNode. Response: {response.json()}")
        else:
            print(f"Failed to send acknowledgment. Status Code: {response.status_code}")

    except requests.RequestException as e:
        print(f"Error sending acknowledgment to NameNode: {e}")

def acknowledge_name_node_ping(name_node_url, data_node_id):
    while True:
        acknowledgment_data = {'data_node_id': data_node_id, 'status': 'active'}

        try:
            response = requests.post(f'{name_node_url}/acknowledge_ping', json=acknowledgment_data)

            if response.status_code == 200:
                print(f"Acknowledgment sent to NameNode for DataNode {data_node_id}")
            else:
                print(f"Failed to send acknowledgment to NameNode for DataNode {data_node_id}. Status Code: {response.status_code}")

        except requests.RequestException as e:
            print(f"Error sending acknowledgment to NameNode: {e}")

        time.sleep(20)  # Adjust the interval as needed

def store_data_block(data, block_id):
    block_path = os.path.join(data_blocks_dir, str(block_id))
    with open(block_path, 'wb') as block_file:
        block_file.write(data)

def retrieve_data_block(data_node_id, block_id):
    #block_path = os.path.join(data_blocks_dir, str(block_id))
    block_path = os.path.join(data_blocks_dir, f'datanode{data_node_id}', f'{block_id}')

    block_path = r"{}".format(block_path)
    print("Reading from:", block_path)
    data = ""
    try:
        with open(block_path, 'rb') as block_file:
            data = block_file.read()
    except IOError as e:
        return 404, ""

    return 200, data

def delete_data_block(block_id):
    block_path = os.path.join(data_blocks_dir, str(block_id))
    if os.path.exists(block_path):
        os.remove(block_path)

def send_acknowledgement(file_path, data_node_id):
    data = {'file_path': file_path, 'data_node_id': data_node_id}
    response = requests.post('http://127.0.0.1:5000/acknowledge_replication', json=data)
    return response.status_code

def replicate_data_block(data_block_path, source_data_node_id):
    for data_node_id in range(1, replication_factor + 1):
        if data_node_id != source_data_node_id:
            destination_path = os.path.join(data_blocks_dir, f'datanode{data_node_id}', os.path.basename(data_block_path))
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            shutil.copy(data_block_path, destination_path)

@app.route('/handle_client_request', methods=['POST'])
def handle_client_request():
    data = request.json
    operation = data.get('operation')
    file_path = data.get('file_path')
    data_node_id = data.get('data_node_id')
    block_id = data.get('block_id')

    if operation == 'read':
        #data_block_path = os.path.join(data_blocks_dir, f'datanode{data_node_id}', f'{block_id}')
        print("******data block:", retrieve_data_block(data_node_id, block_id))
        (status, data_block) = retrieve_data_block(data_node_id, block_id)

        data_block = data_block.decode()       
        data = {"data_block": data_block}
        # Create a custom response object
        response = make_response(data, status)
        # Set the content type to application/octet-stream
        response.headers.set('Content-Type', 'application/octet-stream')
        # Return the response object
        return response

    elif operation == 'write':
        data_block_content = data.get('data_block_content')
        write_data_block(data_node_id, block_id, data_block_content)
        replicate_data_block(os.path.join(data_blocks_dir, f'datanode{data_node_id}', f'{block_id}'), data_node_id)
        status = send_acknowledgement(file_path, data_node_id)
        print("status from send_acknowledgement: ", status)
        if status == 200:
            #calling metadata_update
            data = {'data_node_id': data_node_id, 'file_path': file_path, 'block_id': block_id}
            response = requests.post(f'{name_node_url}/update_metadata', json=data)
            print("response from metadata update: ", response)
            status = response.status_code
        
        return jsonify({"status": status})


def write_data_block(data_node_id, block_id, data_block_content):
    data_block_path = os.path.join(data_blocks_dir, f'datanode{data_node_id}', f'{block_id}')
    os.makedirs(os.path.dirname(data_block_path), exist_ok=True)
    if isinstance(data_block_content, str):
        data_block_content = data_block_content.encode('utf-8')
    with open(data_block_path, 'wb') as file:
        file.write(data_block_content)


if __name__ == '__main__':
    name_node_url = cp.name_node_url
    data_node_host = cp.data_node_host
    data_node_port = cp.data_node_port

    data_node_id = register_with_namenode(name_node_url, data_node_host, data_node_port)

    if data_node_id:
        print(f"Registered with NameNode. Assigned Data Node ID: {data_node_id}")

        ping_thread = threading.Thread(target=acknowledge_name_node_ping, args=(name_node_url, data_node_id))
        ping_thread.daemon = True
        ping_thread.start()

        app.run(host='0.0.0.0', port=data_node_port)
    else:
        print("Failed to register with NameNode")
