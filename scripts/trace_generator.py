
import sys
import argparse
import random

from cityhash import CityHash32, CityHash64
from txn_format_pb2 import Test, Txn, KVPair, OpType
import json
from typing import Union

def reverse_dict(d: dict) -> dict:
  return dict([reversed(i) for i in d.items()])

def accumulate_probability(probability: list[Union[int,float]]) -> list[Union[int,float]]:
  res = [float(probability[0])]
  for i in range(1, len(probability)):
    res.append(res[i - 1] + float(probability[i]))
  return res


MAX_KEY = 2 ** 32 - 1
MAX_VALUE = 2 ** 63 - 1


STR_TO_OP = {
  "put": OpType.OP_PUT,
  "get": OpType.OP_GET,
  "send_and_execute": OpType.OP_SEND_AND_EXECUTE,
  "set": OpType.OP_SET,
  "add": OpType.OP_ADD,
  "sub": OpType.OP_SUB,
  "mul": OpType.OP_MULT,
  "div": OpType.OP_DIV,
  "mod": OpType.OP_MOD,
  "and": OpType.OP_AND,
  "or": OpType.OP_OR,
  "xor": OpType.OP_XOR,
  "not": OpType.OP_NOT,
  "nand": OpType.OP_NAND,
  "nor": OpType.OP_NOR,
}

OP_TO_STR = reverse_dict(STR_TO_OP)

OP_PROBABILITY = dict([(k, 0) for k in OP_TO_STR.keys()])

OP_KEYS: list[OpType] = []
OP_CUMMULATIVE_PROBABILITY: list[float] = []


def init(op_probability_update: dict[OpType, float]) -> None:
  global OP_PROBABILITY
  global OP_KEYS
  global OP_CUMMULATIVE_PROBABILITY
  OP_PROBABILITY.update(op_probability_update)
  OP_KEYS = list(OP_PROBABILITY.keys())
  OP_CUMMULATIVE_PROBABILITY = accumulate_probability(list(OP_PROBABILITY.values()))


init({
  OpType.OP_GET: 0.9,
  OpType.OP_PUT: 0.1,
  OpType.OP_SEND_AND_EXECUTE: 0.05,
})


class Args:
  def __init__(self, **kwargs):
    self.__dict__.update(kwargs)


class KeyRange:
  def __init__(self, begin, keys):
    self.begin = begin
    self.end = begin + keys

def next_rand(mean, sigma = 0) -> int:
  if mean == 0:
    return 0
  res = -1.
  sigma = sigma if sigma != 0 else mean / 4
  while(res < 0):
    res = random.gauss(mean, sigma)
  return int(res)


def seed(seed, dont_print_seed = False) -> None:
  if seed == None:
    seed = random.randrange(sys.maxsize)
  random.seed(seed)
  if not dont_print_seed:
    print(f"Used seed value: {seed}", file=sys.stderr)
  return seed


def generate_fill_trace(key_range: KeyRange) -> Txn:
  res = Txn()
  res.is_txn = False
  res.txn_id = 0
  del res.depends_on[:]
  for i in range(key_range.begin, key_range.end):
    kv = KVPair()
    kv.key = CityHash32((i).to_bytes(4,'little')).to_bytes(4, 'little')
    kv.value = (random.randrange(MAX_VALUE)).to_bytes(8, 'little')
    kv.op = OpType.OP_PUT
    res.cmds.append(kv)
  return res


def generate_txn(tx_id: int, key_range: KeyRange, txn_size: int, depends_on: list[int]) -> Txn:
  res = Txn()
  res.is_txn = True
  res.txn_id = tx_id
  res.depends_on.extend(depends_on)
  i = 0
  while i < txn_size:
    kv = KVPair()
    kv.key = CityHash32((random.randrange(key_range.begin, key_range.end)).to_bytes(4,'little')).to_bytes(4, 'little')
    kv.value = (random.randrange(MAX_VALUE)).to_bytes(8, 'little')
    kv.op = random.choices(OP_KEYS, cum_weights=OP_CUMMULATIVE_PROBABILITY)[0]
    res.cmds.append(kv)
    if kv.op in [OpType.OP_GET, OpType.OP_PUT]:
      i += 1
  return res


def generate_txns(test: Test, key_range: KeyRange, txn_sizes: list[int], num_parallel: int, fill_generated: bool) -> Test:
  i = 0
  depends = [0] if fill_generated else []
  while i < len(txn_sizes):
    count_parallel = next_rand(num_parallel - 1) + 1
    for j in range(min(len(txn_sizes) - i, count_parallel)):
      test.txns.append(generate_txn(i + 1 + j, key_range, txn_sizes[i + j], depends))
    
    depends = list(range(i + 1, i + 1 + count_parallel))
    i += count_parallel
  return test


def write_json(filename: str, args) -> None:
  res = {
    "num_txns": args.num_txns,
    "num_ops": args.num_ops,
    "num_keys": args.num_keys,
    "num_parallel": args.num_parallel,
    "fill_kv_store": args.fill_kv_store,
    "seed": seed(args.seed),
    "op_probability": {OP_TO_STR[k]: v for k,v in OP_PROBABILITY.items()},
  }
  with open(filename, 'w') as f:
    json.dump(res, f, indent=4)


def main(argv: list[str]) -> int:
  parser = argparse.ArgumentParser(description='Generate a trace file for txn_client.')
  parser.add_argument('--num_txns', type=int, default=100, help='Number of transactions to generate.')
  parser.add_argument('--num_ops', type=int, default=10, help='Avg. number of operations per transaction.')
  parser.add_argument('--num_keys', type=int, default=1000, help='Number of keys to generate.')
  parser.add_argument('--num_parallel', type=int, default=1, help='Max. number of transactions to run in parallel.')
  #parser.add_argument('--num_initial_keys', type=int, default=0, help='Number of initial keys to generate.')
  parser.add_argument('--fill-kv-store', action="store_true", help="Will issue a put for every key touched in the transactions before running the experiment")
  #parser.add_argument('--non-txn-ops', type=int, default=0, help='Number of non-txn operations to generate.')
  parser.add_argument('--seed', type=int, default=None, help="Use custom seed instead of system seed")
  parser.add_argument('--dump-config', action="store_true", help="Dump the generated config to stdout, does not perform any generation")
  parser.add_argument('--config', type=str, default=None, help="Use a custom config file instead arguments")
  parser.add_argument('output', type=str, help='Output file.')
  pargs = parser.parse_args(argv)
  if pargs.config is not None:
    print(f"Using config file: {pargs.config}", file=sys.stderr)
    args = Args(**json.load(open(pargs.config)))
    print(**vars(args), file=sys.stderr)
    init(args.op_probability)
  elif pargs.dump_config:
    write_json(pargs.output, pargs)
    return 0
  else:
    args = Args(**vars(pargs))
  seed(args.seed)
  test = Test()
  key_range = KeyRange(random.randrange(MAX_KEY - args.num_keys), args.num_keys)
  txn_sizes = [next_rand(args.num_ops) for _ in range(args.num_txns)]
  if args.fill_kv_store:
    t = generate_fill_trace(key_range)
    test.txns.append(t)
  test = generate_txns(test, key_range, txn_sizes, args.num_parallel, args.fill_kv_store)
  with open(args.output, 'wb') as f:
    f.write(test.SerializeToString())
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))