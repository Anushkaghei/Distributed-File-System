import os
import json
import time
import threading
import uuid
from flask import Flask, request, jsonify
import config_param as cp

app = Flask(__name__)

class DataNode:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host
        self.port = port
        self.last_ping_time = time.time()
        self.files = set()

    def serialize(self):
        return {'id': self.id, 'host': self.host, 'port': self.port, 'files': list(self.files)}

# Define the directories for metadata, data blocks, and Data Node health tracking
metadata_dir = cp.metadata_dir

def initialize_metadata():
    if not os.path.exists(metadata_dir):
        os.makedirs(metadata_dir)

    metadata_file = os.path.join(metadata_dir, cp.metadata_file_name)
    if not os.path.exists(metadata_file):
        metadata = {
            '/': {  # Root directory metadata
                'type': 'directory',
                'files': {},
            }
        }
        save_metadata(metadata)

def save_metadata(metadata):
    metadata_file = os.path.join(metadata_dir, cp.metadata_file_name)
    with open(metadata_file, 'w') as f:  
        json.dump(metadata, f)

def load_metadata():
    metadata_file = os.path.join(metadata_dir, cp.metadata_file_name)
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            return json.load(f)
    else:
        return {}

def update_metadata(file_path, block_id, data_node_id):
    metadata = load_metadata()
    if file_path not in metadata['/']['files']:
        file_path = file_path.replace('\\\\', '\\')
        metadata['/']['files'][file_path] = {'type': 'file', 'blocks': []}

    metadata['/']['files'][file_path]['blocks'].append({'block_id': block_id, 'data_node_id': data_node_id})
    save_metadata(metadata)

@app.route('/update_metadata', methods=['POST'])
def update_metadata_route():
    print("got an update_metadata request: ", request.json)
    data = request.json
    data_node_id = data.get('data_node_id')
    file_path = data.get('file_path')
    block_id = data.get('block_id')

    update_metadata(file_path, block_id, data_node_id)
    return jsonify({'message': 'Metadata updated'}), 200

data_nodes = {}

def ping_data_nodes():
    global data_nodes
    while True:
        if not data_nodes:
            time.sleep(10)
            continue

        for data_node_id in list(data_nodes.keys()):
            last_ping_time = data_nodes[data_node_id].last_ping_time
            if time.time() - last_ping_time > 60:
                handle_data_node_failure(data_node_id)
                del data_nodes[data_node_id]
            else:
                # Update the last_ping_time for active Data Nodes
                data_nodes[data_node_id].last_ping_time = time.time()

        time.sleep(10)


# Start the Data Node ping thread
ping_thread = threading.Thread(target=ping_data_nodes)
ping_thread.daemon = True
ping_thread.start()

def check_data_node_health():
    while True:
        for data_node_id, data_node in data_nodes.items():
            if not ping_data_nodes(data_node):
                handle_data_node_failure(data_node_id)
            else:
                data_node.last_ping_time = time.time()
        time.sleep(60)

# Define connected_clients globally
connected_clients = {}

def check_client_health():
    while True:
        current_time = time.time()
        for client_id, last_ping_time in list(connected_clients.items()):
            if current_time - last_ping_time > 60:
                print(f"Client {client_id} is considered offline.")
                del connected_clients[client_id]
        time.sleep(30)

# Start the client health check thread
health_check_thread = threading.Thread(target=check_client_health)
health_check_thread.daemon = True
health_check_thread.start()

@app.route('/ping', methods=['POST'])
def client_ping():
    client_id = request.json.get('client_id')
    if client_id:
        connected_clients[client_id] = time.time()
        return jsonify({'status': 'success'})
    else:
        return jsonify({'status': 'error', 'message': 'Invalid client ID'}), 400

@app.route('/create_file', methods=['POST'])
def create_new_file():
    data = request.json
    file_path = data.get('file_path')
    print(file_path)
    if file_path:
        # Check if the file already exists
        metadata = load_metadata()
        if file_path not in metadata:
            # File doesn't exist, proceed with creation
            file_id = create_file(file_path)
            return jsonify({'status': 'success', 'file_id': file_id})
        else:
            return jsonify({'status': 'error', 'message': 'File already exists'}), 400
    else:
        return jsonify({'status': 'error', 'message': 'Invalid file path'}), 400

# Function to create a new file in metadata
def create_file(file_path):
    file_id = str(uuid.uuid4())
    update_metadata(file_path, file_id, 0)
    return file_id

@app.route('/get_file_metadata', methods=['GET'])
def get_file_metadata():
    data = request.json
    file_path = data.get('file_path')
    file_path = r"{}".format(file_path)
    metadata = load_metadata()

    if file_path:
        files = metadata.get("/").get("files")
        if file_path in files:
            return jsonify({'status': 'success', 'metadata': files.get(file_path)})
        else:
            return jsonify({'status': 'error', 'message': 'File not found'}), 404
    else:
        return jsonify({'status': 'error', 'message': 'Invalid file path'}), 400


@app.route('/delete_file', methods=['POST'])
def delete_file():
    data = request.json
    file_path = data.get('file_path')

    if file_path:
        delete_status = delete_file_from_metadata(file_path)
        if delete_status:
            return jsonify({'status': 'success', 'message': f'File {file_path} deleted successfully'})
        else:
            return jsonify({'status': 'error', 'message': f'File {file_path} not found'}), 404
    else:
        return jsonify({'status': 'error', 'message': 'Invalid file path'}), 400

# Function to delete a file from metadata
def delete_file_from_metadata(file_path):
    metadata = load_metadata()
    print("metadata: ", metadata)

    if file_path in metadata:
        print("deleting file: ", file_path)
        del metadata[file_path]
        save_metadata(metadata)
        return True
    else:
        return False

@app.route('/move_file', methods=['POST'])
def move_file():
    data = request.json
    src_path = data.get('src_path')
    dest_path = data.get('dest_path')

    if src_path and dest_path:
        move_status = move_file_in_metadata(src_path, dest_path)
        if move_status:
            return jsonify({'status': 'success', 'message': f'File moved from {src_path} to {dest_path}'})
        else:
            return jsonify({'status': 'error', 'message': f'Error moving file or file not found'}), 404
    else:
        return jsonify({'status': 'error', 'message': 'Invalid source or destination path'}), 400

# Function to move a file in metadata
def move_file_in_metadata(src_path, dest_path):
    metadata = load_metadata()

    if src_path in metadata:
        file_metadata = metadata[src_path]
        metadata[dest_path] = file_metadata
        del metadata[src_path]
        save_metadata(metadata)
        return True
    else:
        return False


@app.route('/copy_file', methods=['POST'])
def copy_file():
    data = request.json
    src_path = data.get('src_path')
    dest_path = data.get('dest_path')

    if src_path and dest_path:
        copy_status = copy_file_in_metadata(src_path, dest_path)
        if copy_status:
            return jsonify({'status': 'success', 'message': f'File copied from {src_path} to {dest_path}'}), 200
        else:
            return jsonify({'status': 'error', 'message': f'Error copying file or file not found'}), 404
    else:
        return jsonify({'status': 'error', 'message': 'Invalid source or destination path'}), 400

# Function to copy a file in metadata
def copy_file_in_metadata(src_path, dest_path):
    metadata = load_metadata()
    print("metadata: ", metadata)
    print("src_path: ", src_path)
    print("dest_path: ", dest_path)

    if src_path in metadata:
        print("copying file: ", src_path)
        file_metadata = metadata[src_path]
        metadata[dest_path] = file_metadata.copy()
        save_metadata(metadata)
        return True
    else:
        return False


@app.route('/register', methods=['POST'])
def register_data_node():
    global data_nodes  # Add this line to reference the global variable
    
    print("got a register request: ", request.json)
    data = request.json
    if 'host' in data and 'port' in data:
        if 'data_nodes' not in globals():
            data_nodes = {}
        data_node = DataNode(len(data_nodes) + 1, data['host'], data['port'])
        data_nodes[data_node.id] = data_node
        print(f"Registered DataNode: {data_node.serialize()}")
        print("data_nodes: ", data_nodes)
        return jsonify({'status': 'success', 'data_node': data_node.serialize()})
    else:
        print('Invalid data node information')
        return jsonify({'status': 'error', 'message': 'Invalid data node information'}), 400

@app.route('/acknowledge_ping', methods=['POST'])
def acknowledge_ping():
    data = request.json
    data_node_info = data.get('data_node_id')
    status = data.get('status')

    if status == 'active':
        # Assume that the ping interval should be 60 seconds for active DataNodes
        new_ping_interval = 20
        update_ping_interval(data_node_info, new_ping_interval)
        print(f"Received acknowledgment from DataNode {data_node_info['id']}: {status}. Updated ping interval to {new_ping_interval} seconds.")
    else:
        print(f"Received acknowledgment from DataNode {data_node_info['id']}: {status}.")

    return jsonify({'message': 'Acknowledgment received'}), 200

def update_ping_interval(data_node_info, new_ping_interval):
    data_node_id = data_node_info['id']
    if data_node_id in data_nodes:
        data_nodes[data_node_id].ping_interval = new_ping_interval
    else:
        print(f"DataNode {data_node_id} not found.")

@app.route('/acknowledge_replication', methods=['POST'])
def acknowledge_replication():
    data = request.json
    data_node_id = data.get('data_node_id')
    file_path = data.get('file_path')
    return jsonify({'message': 'Acknowledgment replication received'}), 200

@app.route('/metadata', methods=['GET'])
def get_metadata():
    print("data_nodes: ", data_nodes)
    metadata = {'data_nodes': [node.serialize() for node in data_nodes.values()]}
    app.logger.info('Retrieved metadata')
    return jsonify(metadata)



def handle_data_node_failure(offline_data_node_id):
    print(f"Data Node {offline_data_node_id} is considered dead.")
    replicas_metadata = get_replicas_metadata(offline_data_node_id)
    update_metadata_replicas(replicas_metadata)

def get_replicas_metadata(offline_data_node_id):
    replicas_metadata = {}
    for data_node_id, data_node in data_nodes.items():
        if data_node_id != offline_data_node_id:
            for file_path in data_node.files:
                if file_path not in replicas_metadata:
                    replicas_metadata[file_path] = []
                replicas_metadata[file_path].append(file_path)

    return replicas_metadata

def update_metadata_replicas(replicas_metadata):
    for replica, blocks in replicas_metadata.items():
        update_metadata(replica, blocks)

def resolve_file_path(file_path):
    metadata = load_metadata()
    if file_path in metadata:
        data_blocks = metadata[file_path]
        return data_blocks
    else:
        return None

def send_data_block_to_client(file_path):
    data_nodes_metadata = resolve_file_path(file_path)
    print(f"Sending Data Node metadata for file {file_path} to the client: {data_nodes_metadata}")

@app.route('/list_files', methods=['GET'])
def list_files():
    metadata = load_metadata()
    files_list = list_files_recursive(metadata['/'])
    return jsonify({'files': files_list})

def list_files_recursive(directory):
    files_list = []
    for name, content in directory['files'].items():
        if content['type'] == 'file':
            files_list.append(name)
        elif content['type'] == 'directory':
            subdir_files = list_files_recursive(content)
            files_list.extend([os.path.join(name, file) for file in subdir_files])
    return files_list


if __name__ == '__main__':
    initialize_metadata()
    app.run(host=cp.name_node_host, port=cp.name_node_port)
