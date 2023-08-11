# get delete set begin commit rollback

class KVS:

    def __init__(self):
        self.transactions = [] # ephemeral, stack
        self.db = {} # non-ephemeral

    def begin(self):
        self.transactions.append({})

    def rollback(self):
        self.transactions.pop()

    def commit(self):
        if self.transactions:
            last_transactions = self.transactions.pop()
            for k, v in last_transactions.items():
                self.db[k] = v
                if self.transactions and k in self.transactions[-1]:
                    self.transactions[-1][k] = v

    def set(self, key, value):
        self.transactions[-1][key] = value

    def get(self, key):
        if key in self.db:
            return self.db[key]
        elif self.transactions and key in self.transactions[-1]:
            return self.transactions[-1][key]
        return None

    def delete(self, key):
        if self.transactions and key in self.transactions[-1]:
            del self.transactions[-1][key]
        elif key in self.db:
            del self.db[key]


kv = KVS()
# plan vanilla, set a k,v, get, commit
kv.begin()
kv.set("a", 1)
assert kv.get("a") == 1
kv.delete("a")
kv.commit()
assert kv.get("a") is None

# set, get, rollback
kv.begin()
kv.set("b", 2)
kv.set("c", 3)
assert kv.get("b") == 2
assert kv.get("c") == 3
kv.rollback()
assert kv.get("b") is None
assert kv.get("c") is None

# nested transactions
kv.begin()
kv.set("d", 4)
kv.set("e", 5)
kv.begin()
assert kv.get("d") is None
assert kv.get("e") is None
kv.set("d", 4)
kv.set("e", 5)
kv.set("a", 100) # reset previous value
assert kv.get("d") == 4
assert kv.get("e") == 5
assert kv.get("a") == 100
kv.commit()
assert kv.get("a") == 100
assert kv.get("d") == 4
assert kv.get("e") == 5
kv.rollback()
assert kv.get("a") == 100
assert kv.get("d") == 4
assert kv.get("e") == 5
