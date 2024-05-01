import os
import httpx
import asyncio
import time
import logging

SOURCE_FILENAME = "sideA-AB-line12_person_A124879.txt"
btc_filename = "btc.txt"
eth_filename = "eth.txt"
btc_results_filename = "btc_results.txt"
eth_results_filename = "eth_results.txt"
results_filename = "results.txt"


ETHER_SCAN_API_KEY=os.getenv('ETHER_SCAN_API_KEY')

ETHER_API_RATE_LIMIT = 5 # requests per second
ETHER_MAX_PACK_SIZE = 20 # adresses in one request
ETHER_SLEEP_INTERVAL = 1 / ETHER_API_RATE_LIMIT
BTC_API_RATE_LIMIT = 5 # requests per second
BTC_MAX_PACK_SIZE = 20 # adresses in one request
BTC_SLEEP_INTERVAL = 1 / BTC_API_RATE_LIMIT

# Semaphores for each API
ether_semaphore = asyncio.Semaphore(ETHER_API_RATE_LIMIT)
btc_semaphore = asyncio.Semaphore(BTC_API_RATE_LIMIT)


logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname).3s | %(name)s -> %(funcName)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
fmt = logging.Formatter(fmt="%(asctime)s %(levelname).3s | %(name)s -> %(funcName)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
# c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('btc-eth-scan.log')
f_handler.setLevel(logging.WARNING)
# c_handler.setLevel(logging.DEBUG)
logger = logging.getLogger("btc-eth-scan")

# logger.addHandler(c_handler)
logger.addHandler(f_handler)

for handler in logger.handlers:
    handler.setFormatter(fmt)



async def get_ether_balance(address: str, client: httpx.AsyncClient):
    ETHER_VALUE_IN_WEI = 10 ** 18
    balance_in_wei = await get_ether_balance_in_wei(address, client)
    balance = balance_in_wei / ETHER_VALUE_IN_WEI
    print(f"{address} balance: {balance} ETH")
    return balance


async def get_ether_balance_in_wei(addresses_pack: str, client: httpx.AsyncClient, semaphore: asyncio.Semaphore = ether_semaphore, sleep_interval: float = ETHER_SLEEP_INTERVAL):
    addresses = (",".join(addresses_pack))
    chain = "ETH"
    url = f"https://api.etherscan.io/api?module=account&action=balancemulti&address={addresses}&tag=latest&apikey={ETHER_SCAN_API_KEY}"

    async with semaphore:
        start_time = time.time()
        await asyncio.sleep(sleep_interval - (time.time() - start_time))
        try:
            response = await client.get(url)
        except httpx.RequestError as e:
            logger.error(e)
            await asyncio.sleep(sleep_interval)
            return None
        elapsed = time.time() - start_time
        sleep_duration = max(0, sleep_interval - elapsed)
        await asyncio.sleep(sleep_duration)
        if response.status_code != 200:
            logger.error(f"{chain} {addresses}, Error: {response.status_code} - {response.text}")
            return None
        result = response.json()
        if result["status"] != "1":
            logger.error(f"{chain} {addresses}, Error: {result}")
            return None
        results = []
        for item in result.get("result"):
            if item.get("balance") != "0":
                results.append(
                    {"chain": chain, "address": item.get("account"), "balance": item.get("balance")}
                )
        return results
    

async def get_btc_balance(addresses_pack: str, client: httpx.AsyncClient, semaphore: asyncio.Semaphore = btc_semaphore, sleep_interval: float = BTC_SLEEP_INTERVAL):
    addresses = ("|".join(addresses_pack))
    chain = "BTC"
    url = f"https://blockchain.info/balance?active={addresses}"
    print(url)
    async with semaphore:
        start_time = time.time()
        await asyncio.sleep(sleep_interval - (time.time() - start_time))
        try:
            response = await client.get(url)
        except httpx.RequestError as e:
            logger.error(e)
            await asyncio.sleep(sleep_interval)
            return None
        elapsed = time.time() - start_time
        sleep_duration = max(0, sleep_interval - elapsed)
        await asyncio.sleep(sleep_duration)
        if response.status_code != 200:
            logger.error(f"{chain} {addresses}, Error: {response.status_code} - {response.text}")
            return None
        result = response.json()
        print(result)
        results = []
        for item in result.get("addresses"):
            if item.get("final_balance") != 0:
                results.append(
                    {"chain": chain, "address": item.get("address"), "balance": str(item.get("final_balance"))}
                )
        return results


async def main():
    async with httpx.AsyncClient() as client:
        eth_addresses = set()
        btc_addresses = set()

        with open(SOURCE_FILENAME) as f:
            for line in f:
                if "BTC address" in line:
                    btc_address = line.split(": ")[1].strip()
                    btc_addresses.add(btc_address)                   
                elif "ETH address" in line:
                    eth_address = line.split(": ")[1].strip()
                    eth_addresses.add(eth_address)
        tasks = []
        eth_addresses_list = list(eth_addresses)
        btc_addresses_list = list(btc_addresses)
        for address_pack in range(0, len(eth_addresses_list), ETHER_MAX_PACK_SIZE):
            addresses = eth_addresses_list[address_pack:address_pack + ETHER_MAX_PACK_SIZE]
            tasks.append(asyncio.create_task(get_ether_balance_in_wei(
                        addresses_pack=addresses, 
                        client=client, 
                        semaphore=ether_semaphore, 
                        sleep_interval=ETHER_SLEEP_INTERVAL
                    )))
            
        for address_pack in range(0, len(btc_addresses_list), BTC_MAX_PACK_SIZE):
            addresses = btc_addresses_list[address_pack:address_pack + BTC_MAX_PACK_SIZE]
            tasks.append(asyncio.create_task(get_btc_balance(
                        addresses_pack=addresses, 
                        client=client, 
                        semaphore=btc_semaphore, 
                        sleep_interval=BTC_SLEEP_INTERVAL
                    )))

        results = await asyncio.gather(*tasks)
        btc_results=set()
        eth_results=set()
        for result in results:
            if result:
                if result is None:
                    continue
                for item in result:
                    if item["chain"] == "BTC":
                        btc_results.add(item["address"])
                    elif item["chain"] == "ETH":
                        eth_results.add(item["address"])
                    else:
                        logger.error(f"Unknown chain: {item}")
                        continue
        results = eth_results.union(btc_results)
        with open(results_filename, 'w') as f:
            for address in results:
                f.write(f"{address}\n")


if __name__ == "__main__":
    asyncio.run(main())