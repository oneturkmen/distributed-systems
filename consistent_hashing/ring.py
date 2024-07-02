import bisect
import hashlib
from typing import List


class ServerNode:
    def __init__(self, name, data):
        self.name = name
        self.data = data
    
    def insert_data(self, key: str, value: int):
        """ Store data in the server. """
        if key in self.data:
            print(f"Warning! Overriding {key} in {self.name} server: {value=}")
        self.data[key] = value
    
    def remove_data(self, key: str):
        """ Remove data from the server. """
        print(f"Removing {key} from {self.name} server")
        del self.data[key]

class ConsistentHashingRing:
    def __init__(self):
        self.ring = dict()
        self.sorted_server_keys = []
        self.hash_fn = lambda key: int(hashlib.md5(str(key).encode("utf-8")).hexdigest(), 16) % 360

    def get_successor_node(self, node: ServerNode) -> ServerNode:
        """ Given a node, get its successor on the ring. """
        hashed_key = self.hash_fn(node.name)
        successor_idx = bisect.bisect(self.sorted_server_keys, hashed_key) % len(self.sorted_server_keys)
        return self.ring[self.sorted_server_keys[successor_idx]]

    def transfer_data(self, from_node: ServerNode, to_node: ServerNode, keys: List[str]):
        """ Transfers keys (data) from one node to another. """
        for key in keys:
            # Transfer data to the newly added node from the successor node.
            to_node.insert_data(key, from_node.data[key])
        for key in keys:
            # Remove data from the successor node to avoid unnecessary duplication.
            from_node.remove_data(key)
    
    def add_server(self, node: ServerNode):
        """ Add a server to the ring. """
        hashed_key = self.hash_fn(node.name)

        if hashed_key in self.ring:
            raise KeyError(f"{hashed_key=} already exists on the ring.")

        self.ring[hashed_key] = node 
        bisect.insort(self.sorted_server_keys, hashed_key)

        # Find the successor node from which we will grab the keys.
        successor_node = self.get_successor_node(node)

        # Find relevant keys to transfer
        keys_to_transfer = [
            key for key in successor_node.data.keys() if self.hash_fn(key) < self.hash_fn(node.name)
        ]

        self.transfer_data(from_node=successor_node, to_node=node, keys=keys_to_transfer)

    def remove_server(self, node: ServerNode):
        """ Remove a server from the ring. """
        hashed_key = self.hash_fn(node.name)

        # First, move data to the successor node.
        successor_node = self.get_successor_node(node)
        keys_to_transfer = [key for key in node.data.keys()]
        self.transfer_data(from_node=node, to_node=successor_node, keys=keys_to_transfer)

        # Delete information about the node.
        del self.ring[hashed_key]
        self.sorted_server_keys.remove(hashed_key)

    def add_data(self, key: str, value: int):
        """ Add data to the ring & store it in correct server. """
        hashed_key = self.hash_fn(key)
        server_index = bisect.bisect(self.sorted_server_keys, hashed_key)

        print(f"{key=},{hashed_key=}")

        server_index = server_index % len(self.sorted_server_keys)

        target_node = self.ring[self.sorted_server_keys[server_index]]
        target_node.insert_data(key, value)

    def remove_data(self, key: str):
        """ Remove data from the ring from the correct server node. """
        hashed_key = self.hash_fn(key)
        server_index = bisect.bisect(self.sorted_server_keys, hashed_key)

        server_index = server_index % len(self.sorted_server_keys)
        
        target_node = self.ring[self.sorted_server_keys[server_index]]
        target_node.remove_data(key)
    
    def print_debug(self):
        """ Print some debugging info """
        print("Printing servers info ...")
        for hashed_key, node in self.ring.items():
            print(f"{hashed_key=}, {node.name=}")
        print(self.sorted_server_keys)

        print("Printing data assigned to servers")
        for _, node in self.ring.items():
            print(f"{node.name=}, {node.data=}")

ring = ConsistentHashingRing()
servers = [
    ServerNode(name="can1", data=dict()),
    ServerNode(name="can2", data=dict()),
    ServerNode(name="can3", data=dict()),
    ServerNode(name="can4", data=dict()),
    # ServerNode(name="can5", data=dict()),
    ServerNode(name="can6", data=dict()),

]
for server in servers:
    ring.add_server(server)

ring.add_data("key1", 5)
ring.add_data("key2", 6)
ring.add_data("key3", 7)
ring.add_data("key4", 8)
ring.add_data("key5", 8)
ring.add_data("key6", 8)
ring.add_data("key7", 8)
# ring.add_data("key4", 8)

print("Before server insertion")
ring.print_debug()
print("----"*20)
new_server = ServerNode(name="can5", data=dict())
ring.add_server(new_server)
print("After server insertion")
ring.print_debug()
print("----"*20)
print("After server removal")
ring.remove_server(new_server)
ring.print_debug()