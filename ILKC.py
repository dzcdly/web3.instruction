import random
import time
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3
from datetime import datetime
from requests.exceptions import ReadTimeout

# 定义多个节点
nodes = [
    # "https://polygon-mainnet.public.blastapi.io"
    "https://go.getblock.io/9e55528ab14444f290c5ab15f73c8975",
]
gas_x = 1.02  # gas_price倍率
gas_limit = 23500   # gas_limit值
chain_id = 137   # KCS链ID
hex_datas = "0x646174613a2c7b2270223a227072632d3230222c226f70223a226d696e74222c227469636b223a22706f6c6d222c22616d74223a2231303030227d"

def get_web3_instance():
    while True:
        for node_url in nodes:
            try:
                # 尝试连接到节点
                w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={'timeout': 2}))
                if w3.is_connected():
                    print(f"Connected to node: {node_url}")
                    return w3
            except Exception as e:
                print(f"Failed to connect to node {node_url}: {e}")
                time.sleep(6)

def send_transaction(w3, i, source_wallet, amount_in_wei, hex_data, private_key):
    # source_wallets = "0x83b978Cf73ee1D571b1a2550c5570861285AF337"
    gas = int((w3.eth.gas_price)*gas_x)
    try:
        current_nonce = w3.eth.get_transaction_count(source_wallet,'pending')
        # 构建交易
        transaction = {
            'to': source_wallet,  # 目标地址改为当前迭代的地址
            'value': amount_in_wei,
            'gas': gas_limit,
            'gasPrice': gas,  # 替换为适当的 gas 价格
            'nonce': current_nonce,  # 使用获取的 nonce
            'data': hex_data,
            'chainId': chain_id  #  链ID
        }

        # 签名交易
        signed_transaction = w3.eth.account.sign_transaction(transaction, private_key)
        # 发送交易
        transaction_hash = w3.eth.send_raw_transaction(signed_transaction.rawTransaction)

        # 打印交易信息
        print(f'钱包{i}向自己发起转账', f'发送交易时间：{datetime.now()}', f'执行nonce: {current_nonce}', f'交易哈希: {transaction_hash.hex()}')
    except ReadTimeout as e:
        print(f'Read timeout: {e}')
        # 切换到下一个可用节点
        w3 = get_web3_instance()
    except ValueError as e:
        if 'nonce too low' in str(e):
            # 如果遇到 "nonce too low" 错误，重新获取最新的 nonce
            print('Nonce too low. Reattempting with a new nonce.')
            current_nonce = w3.eth.get_transaction_count(source_wallet,'pending')
            # 重新构建交易
            transaction = {
                'to': source_wallet,
                'value': amount_in_wei,
                'gas': gas_limit,
                'gasPrice': gas,
                'nonce': current_nonce,
                'data': hex_data,
                'chainId': chain_id   # 链ID
            }

            # 签名交易
            signed_transaction = w3.eth.account.sign_transaction(transaction, private_key)

            # 发送交易
            transaction_hash = w3.eth.send_raw_transaction(signed_transaction.rawTransaction)

            # 打印交易信息
            print(f'Reattempted - 钱包{i}向自己发起转账', f'发送交易时间：{datetime.now()}',
                  f'执行nonce: {current_nonce}', f'交易哈希: {transaction_hash.hex()}')


wallet_info = [
    {"address": "地址1", "private_key": "对应私钥1"},
    {"address": "地址2", "private_key": "对应私钥2"},
    {"address": "地址3", "private_key": "对应私钥3"},
    {"......"}
]

def main():
    amount_in_wei = 0
    hex_data = hex_datas
    w3 = get_web3_instance()
    with ThreadPoolExecutor() as executor:
        while True:
            gas_value = w3.from_wei(w3.eth.gas_price, 'gwei')
            if gas_value < 225:
                # 使用多线程并发执行
                executor.map(lambda x: send_transaction(w3, x["index"], x["address"], amount_in_wei, hex_data, x["private_key"]),
                              [{"index": i, **info} for i, info in enumerate(wallet_info, start=1)])
                # sj = random.randint(150,350)
                time.sleep(1)
            else:
                print(f'当前gas{gas_value}', f'超过设定值25，等待中')
                time.sleep(30)
                w3 = get_web3_instance()

# 运行同步程序
if __name__ == "__main__":
    main()