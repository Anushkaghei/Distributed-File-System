import requests
from collections import defaultdict
import time
import threading
import os
import config_param as cp

name_node_url = cp.name_node_url  

current_node_index = 0  # To keep track of the current data node to upload a block
data_nodes = []


def connect_to_namenode():
    # Send connection request to Namenode and retrieve available Datanodes
    response = requests.get(f'{name_node_url}/metadata')
    print("Response from metadata: ", response)
    if response.status_code == 200:
        data_nodes = response.json().get('data_nodes')
        print("Pinging Namenode")
        return data_nodes
    else:
        print("Failed to connect to Namenode")
        return None


def split_file_into_blocks(file_path, block_size=10):
    block_list = []
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        num_lines = len(lines)
        num_blocks = num_lines // block_size + (num_lines % block_size > 0)
        for i in range(num_blocks):
            start_idx = i * block_size
            end_idx = min((i + 1) * block_size, num_lines)
            block_list.append(''.join(lines[start_idx:end_idx]))
        print(f"File '{file_path}' split into {num_blocks} blocks")
    return block_list


def retrieve_free_datanode(data_nodes):
    # Retrieve a free Datanode endpoint from the Namenode
    for data_node in data_nodes:
        data_node_url = f'http://{data_node["host"]}:{data_node["port"]}/status'
        response = requests.get(data_node_url)
        if response.status_code == 200 and response.json().get('status') == 'active':
            print(f"Retrieved free Datanode: {data_node_url}")
            return data_node_url
    return None


def upload_block(data_node_url, block_id, block_data):
    try:
        data = {'operation': 'write', 'file_path': file_path, 'data_node_id': 1, 'block_id': block_id,
                'data_block_content': block_data}
        print("Uploading to data_node_url: ", data_node_url)
        response = requests.post(f'{data_node_url}/handle_client_request', json=data)
        if response.status_code == 200:
            print(f"Block {block_id} uploaded to {data_node_url}")
            return True
        else:
            print(f"Failed to upload Block {block_id} to {data_node_url}. Status Code: {response.status_code}")
            return False
    except requests.RequestException as e:
        print(f"Error uploading Block {block_id} to {data_node_url}: {e}")
        return False


def send_file_to_datanode(file_path, data_nodes):
    global current_node_index
    blocks = split_file_into_blocks(file_path)
    num_nodes = len(data_nodes)
    file_name = os.path.basename(file_path)

    for idx, block in enumerate(blocks):
        block_id = f"{file_name}_block_{idx + 1}"
        current_node = data_nodes[current_node_index]
        data_node_url = f'http://{current_node["host"]}:{current_node["port"]}'
        
        print(f"Uploading block {block_id} to Datanode: {data_node_url}")
        success = upload_block(data_node_url, block_id, block)
        
        if success:
            print(f"Finished uploading block {block_id} to Datanode: {data_node_url}")
            current_node_index = (current_node_index + 1) % num_nodes  # Move to the next data node
    
    return True


# Download protocols
def request_file_download(file_name):
    # Request the Namenode for Datanode URLs where the file is present
    data = {"file_path": file_name}
    response = requests.get(f'{name_node_url}/get_file_metadata', json=data)
    #print("***response:", response.json())

    if response.status_code == 200:
        data_node_details = response.json().get('metadata', [])
        blocks = data_node_details.get('blocks')
        #print("********blocks:", blocks)
        # Form all data node urls
        data_node_urls = []
        blocks_data = defaultdict(list)

        for idx, block in enumerate(blocks):
            block_id = block.get('block_id')
            current_node = data_nodes[current_node_index]
            data_node_url = f'http://{current_node["host"]}:{current_node["port"]}'

            block_data = download_block(data_node_url, block_id)
            if block_data:
                blocks_data[block_id].append(block_data)
                #print("****data received:", blocks_data[block_id])

        return block_data
    else:
        print(f"Failed to get Datanode URLs for file '{file_name}'")
        return []


def download_block(data_node_url, block_id):
    try:
        data = {'operation': 'read', 'file_path': file_path, 'data_node_id': 1, 'block_id': block_id}
        print("Reading from data_node_url: ", data_node_url)
        response = requests.post(f'{data_node_url}/handle_client_request', json=data)
        if response.status_code == 200:
            print(f"Block {block_id} read from {data_node_url} data {response.content}")
            return True
        else:
            print(f"Failed to read Block {block_id} from {data_node_url}. Status Code: {response.status_code}")
            return False
    except requests.RequestException as e:
        print(f"Error reading Block {block_id} to {data_node_url}: {e}")
        return False


def rearrange_blocks(file_name, blocks):
    # Rearrange blocks in the order of their IDs
    sorted_blocks = []
    block_id = 1
    while True:
        block = blocks.get(f"{file_name}block{block_id}")
        if block:
            sorted_blocks.extend(block)
            block_id += 1
        else:
            break
    return sorted_blocks


def verify_blocks(blocks):
    # Verify if all blocks are received
    expected_block_ids = set(f"block_{i}" for i in range(1, len(blocks) + 1))
    received_block_ids = set(blocks.keys())
    return expected_block_ids == received_block_ids


def send_acknowledgement(successful):
    # Send acknowledgement to the server
    if successful:
        print("File download successful. Sending acknowledgement...")
    else:
        print("File download unsuccessful")


def download_file(file_name):
    data_blocks = request_file_download(file_name)
    status = True
    # if data_blocks:
    #     sorted_blocks = rearrange_blocks(file_name, data_blocks)
    #     if verify_blocks(data_blocks):
    #         print("All blocks received and arranged successfully")
    #         status = True
    #     else:
    #         status = False
    send_acknowledgement(status)


import copy
def client_operations():
        while True:
            data_nodes_copy = copy.deepcopy(data_nodes)

            print("data_nodes: ", data_nodes_copy)
            if data_nodes_copy:
                nodes_to_remove = []
                
                for node in data_nodes_copy:
                    data_node_url = f'http://{node["host"]}:{node["port"]}'
                    print(f"Uploading to Datanode: {data_node_url}")
                    success = send_file_to_datanode(file_path, data_node_url)

                    if success:
                        nodes_to_remove.append(node)
                    else:
                        print("Aborting further uploads due to failure")
                        break
            # print(f"Downloading file: {file_name}")
                for node in nodes_to_remove:
                    data_nodes_copy.remove(node)
                    print(f"Removed node from data_node_copy: {node}")
            # Add a delay before the next iteration
            time.sleep(5) 


def list_all_files():
    response = requests.get(f'{name_node_url}/list_files')
    if response.status_code == 200:
        files = response.json()
        print(files)
    else:
        print(f"Failed to get files. Status Code: {response.status_code}")

def create_file(file_path):
    response = requests.post(f'{name_node_url}/create_file', json={'file_path': file_path})
    if response.status_code == 200:
        print(f"File '{file_path}' created successfully.")
    else:
        print(f"Failed to create a new file. Status Code: {response.status_code}")

def delete_file(file_path):
    response = requests.post(f'{name_node_url}/delete_file', json={'file_path': file_path})
    if response.status_code == 200:
        print(f"File '{file_path}' deleted successfully.")
    else:
        print(f"Failed to delete file. Status Code: {response.status_code}")

def move_file(src_path, dest_path):
    response = requests.post(f'{name_node_url}/move_file', json={'src_path': src_path, 'dest_path': dest_path})
    if response.status_code == 200:
        print(f"File '{src_path}' moved to '{dest_path}' successfully.")
    else:
        print(f"Failed to move file. Status Code: {response.status_code}")

def copy_file(src_path, dest_path):
    response = requests.post(f'{name_node_url}/copy_file', json={'src_path': src_path, 'dest_path': dest_path})

    if response.status_code == 200:
        print(f"File '{src_path}' copied to '{dest_path}' successfully.")
    else:
        print(f"Failed to copy file. Status Code: {response.status_code}")

def traverse_directory(directory_path):
    response = requests.post(f'{name_node_url}/traverse_directory', json={'directory_path': directory_path})

    if response.status_code == 200:
        print("\nFiles and Directories in the specified directory:")
        print(response.json())
    else:
        print(f"Failed to traverse directory. Status Code: {response.status_code}")


    

if __name__ == "__main__":
    current_node_index = 0  # To keep track of the current data node to upload a block
    data_nodes = connect_to_namenode()
    
    while True:
        print("\nChoose a client action to perform:")
        print("1. Upload a file")
        print("2. Download a file")
        print("3. List all files and directories")
        print("4. Create a file")
        print("5. Delete a file")
        # print("6. Move a file")
        # print("7. Copy a file")

        print("0. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            file_path = input("Enter the path of the file to upload: ")
            send_file_to_datanode(file_path,data_nodes)
        elif choice == '2':
            file_path = input("Enter the path of the file to download: ")
            download_file(file_path)
                          
        elif choice == '3':
            list_all_files()
        elif choice == '4':
            file_path = input("Enter the path of the file to create: ")
            create_file(file_path)
        elif choice == '5':
            file_path = input("Enter the path of the file to delete: ")
            delete_file(file_path)
        elif choice == '0':
            print("Exiting the client program.")
            break
        else:
            print("Invalid choice. Please enter a valid option.")

    # Create a separate thread for the client operations
    client_thread = threading.Thread(target=client_operations)
    client_thread.start()

    # Main thread can do other things or just wait for the client thread to finish
    client_thread.join()
