import txn_format_pb2

import sys

def write_small_test():
  test = txn_format_pb2.Test()
  for i in range(3):
    txn = txn_format_pb2.Txn()
    txn.txn_id = i
    if i != 0:
      txn.depends_on.append(0)
    for j in range(3):
      kv = txn_format_pb2.KVPair()
      kv.key = (j).to_bytes(1, 'little')
      print(kv.key)
      kv.value = (j + i * 3).to_bytes(1, 'little')
      kv.op = txn_format_pb2.OP_PUT if j % 2 == 0 else txn_format_pb2.OP_GET
      txn.cmds.append(kv)
    test.txns.append(txn)
  with open('small_test.bin', 'wb') as f:
    f.write(test.SerializeToString())

write_small_test()