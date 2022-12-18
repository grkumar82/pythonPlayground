'''
Implement LRU cache using LinkedList
'''


class Node:

    def __init__(self, key=None, val=None):
        self.key = key
        self.val = val
        # pointers
        self.next, self.prev = None, None


class LRUCache:

    def __init__(self, capacity):
        self.capacity = capacity
        self.size = 0
        self.keyToNode = {}
        self.head, self.tail = Node(), Node()
        # link head and tail nodes
        self.head.next, self.tail.prev = self.tail, self.head

    def addToTail(self, key, value):
        node = Node(key, value)
        node.next = self.tail
        node.prev = self.tail.prev
        # insert prior to tail
        self.tail.prev.next, self.tail.prev = node, node
        self.keyToNode[key] = node
        self.size += 1

    def delete(self, key):
        node = self.keyToNode[key]
        del self.keyToNode[key]
        # re-adjust pointers
        nextNode, prevNode = node.next, node.prev
        nextNode.prev = prevNode
        prevNode.next = nextNode
        self.size -= 1


    def get(self, key):
        if key not in self.keyToNode:
            return -1
        value = self.keyToNode[key].val
        self.delete(key)
        self.addToTail(key, value)
        return value

    def put(self, key, value):
        if key in self.keyToNode:
            self.delete(key)
            self.addToTail(key, value)
            return
        if self.size == self.capacity:
            self.delete(self.head.next.key)
        self.addToTail(key, value)

lRUCache = LRUCache(2);
lRUCache.put(1, 1);  # cache is {1=1}
lRUCache.put(2, 2);  # cache is {1=1, 2=2}
print(lRUCache.get(1));  # return 1
lRUCache.put(3, 3);  # LRU key was 2, evicts key 2, cache is {1=1, 3=3}
print(lRUCache.get(2));  # returns -1 (not found)
lRUCache.put(4, 4);  # LRU key was 1, evicts key 1, cache is {4=4, 3=3}
print(lRUCache.get(1));  # return -1 (not found)
print(lRUCache.get(3));  # return 3
print(lRUCache.get(4));  # return 4
