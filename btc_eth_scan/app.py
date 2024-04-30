import os
import httpx
import asyncio
import time
import logging

filename = "sideA-AB-line12_person_A124879.txt"
btc_filename = "btc.txt"
eth_filename = "eth.txt"
btc_results_filename = "btc_results.txt"
eth_results_filename = "eth_results.txt"
results_filename = "results.txt"


ETHER_SCAN_API_KEY=os.getenv('ETHER_SCAN_API_KEY')

ETHER_API_RATE_LIMIT = 5 # requests per second
ETHER_MAX_PACK_SIZE = 20 # adresses in one request
ETHER_SLEEP_INTERVAL = 1 / ETHER_API_RATE_LIMIT
BTC_API_RATE_LIMIT = 10 # requests per second

BTC_SLEEP_INTERVAL = 1 / BTC_API_RATE_LIMIT

# Semaphores for each API
ether_semaphore = asyncio.Semaphore(ETHER_API_RATE_LIMIT)
btc_semaphore = asyncio.Semaphore(BTC_API_RATE_LIMIT)


logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname).3s | %(name)s -> %(funcName)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
fmt = logging.Formatter(fmt="%(asctime)s %(levelname).3s | %(name)s -> %(funcName)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('btc-eth-scan.log')
f_handler.setLevel(logging.WARNING)
logger = logging.getLogger("btc-eth-scan")

logger.addHandler(c_handler)
logger.addHandler(f_handler)

for handler in logger.handlers:
    handler.setFormatter(fmt)
logger.setLevel(logging.DEBUG)



async def get_ether_balance(address: str, client: httpx.AsyncClient):
    ETHER_VALUE_IN_WEI = 10 ** 18
    balance_in_wei = await get_ether_balance_in_wei(address, client)
    balance = balance_in_wei / ETHER_VALUE_IN_WEI
    print(f"{address} balance: {balance} ETH")
    return balance


async def get_ether_balance_in_wei(address: str, client: httpx.AsyncClient, semaphore: asyncio.Semaphore = ether_semaphore, sleep_interval: float = ETHER_SLEEP_INTERVAL):
    chain = "ETH"
    url = f"https://api.etherscan.io/api?module=account&action=balance&address={address}&tag=latest&apikey={ETHER_SCAN_API_KEY}"
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
            logger.error(f"{chain} {address}, Error: {response.status_code} - {response.text}")
            return None
        result = response.json()
        if result["status"] != "1":
            logger.error(f"{chain} {address}, Error: {result}")
            return None
        balance = int(response.json()["result"])
        return {"chain": chain, "address": address, "balance": balance}
    


async def get_btc_balance(address: str, client: httpx.AsyncClient):
    return {"chain": "BTC", "address": address, "balance": "0"}


async def main():
    async with httpx.AsyncClient() as client:
        tasks = []
        with open(filename) as f:
            for line in f:
                if "BTC address" in line:
                    btc_address = line.split(": ")[1].strip()
                    tasks.append(asyncio.create_task(get_btc_balance(btc_address, client)))
                elif "ETH address" in line:
                    eth_address = line.split(": ")[1].strip()
                    tasks.append(asyncio.create_task(get_ether_balance_in_wei(
                        address=eth_address, 
                        client=client, 
                        semaphore=ether_semaphore, 
                        sleep_interval=ETHER_SLEEP_INTERVAL
                    )))
        results = await asyncio.gather(*tasks)
        btc_results=set()
        eth_results=set()
        for result in results:
            if result:
                if result is None:
                    continue
                if result.get("balance") is None:
                    continue
                if result.get("balance") == "0":
                    continue

                if result["chain"] == "BTC":
                    btc_results.add(result["address"])
                elif result["chain"] == "ETH":
                    eth_results.add(result["address"])
                else:
                    logger.error(f"Unknown chain: {result}")
                    continue
        results = eth_results.union(btc_results)
        with open(results_filename, 'w') as f:
            for address in results:
                f.write(f"{address}\n")


if __name__ == "__main__":
    asyncio.run(main())