from concurrent.futures import ThreadPoolExecutor
from modules.swapper import *
from modules.path_generator import *
from modules.subs_withdrawal import okx_transfer


def path_processing(private_key, file_path):
    with open(file_path, "r") as f:
        paths = json.load(f)
    for i, el in enumerate(paths):
        result = False
        status = el.get("status")
        if status is None or status != "Success":
            if status == "Pending":
                if "hash" in el and get_transaction_status(el["hash"]):
                    print("\n >>> Транзакция подтверждена, обновляем статус на Success")
                    update_status_and_write(paths, file_path, i, "Success")
                    continue
            if el['type'] == 'WITHDRAWAL':
                delay(delay_txn)
                to_token = el['to_token'][0]
                to_chain = el['to']
                amount = el['amount']
                if el['project'] == 'amount in' or el['project'] == 'native':
                    exchange = el['exchange']
                    result = call_exchange_withdraw(exchange, private_key, amount, to_token, to_chain, paths, file_path, i)
                if el['project'] == 'amount out':
                    if el['address'] is not None:
                        withdrawal_address = el['address']
                        result = withdrawal_from(private_key, withdrawal_address, to_chain, to_token, paths, file_path, i)
                    print_with_time(f"  Закончил работу с кошельком...")
                    print(f"          {get_address_wallet(private_key)}\n", flush=True)
            if el['type'] == 'Bridge':
                delay(delay_txn)
                from_chain = el['from']
                to_chain = el['to']
                from_token = el['token'][0]
                to_token = el['to_token'][0]
                if el['project'] == 'Core' and el['from'] == 'BSC':
                    result = core_bridge(private_key, from_chain, from_token, paths, file_path, i)
                if el['project'] == 'Core' and el['from'] == 'Core':
                    result = from_core_bridge(private_key, from_token, from_chain, paths, file_path, i)
                if el['project'] == 'Harmony':
                    amount = el['amount']
                    harmony_bridge(private_key, amount, from_chain, from_token, paths, file_path, i)
                if el['project'] == 'Stargate':
                    result = stargate_bridge(private_key, from_chain, to_chain, from_token, to_token, paths, file_path, i)
                if el['project'] == 'BTCb':
                    btcb_bridge(private_key, from_chain, to_chain, from_token, paths, file_path, i)
                if el['project'] == 'Testnet':
                    amount = el['amount']
                    testnet_bridge(private_key, amount, from_chain, "ETH", paths, file_path, i)
                if el['project'] == 'Aptos':
                    amount = el['amount']
                    aptos_address = el['address']
                    aptos_bridge(private_key, amount, from_chain, from_token, aptos_address, paths, file_path, i)
            if el['type'] == 'Buy':
                delay(delay_txn)
                from_chain = el['from']
                from_token = el['token'][0]
                to_token = el['to_token'][0]
                amount = el['amount']
                choose_and_call_swap(private_key, from_token, from_chain, amount, to_token, paths, file_path, i)
            if el['type'] == 'Staking':
                delay(delay_txn)
                from_chain = el['from']
                from_token = el['token'][0]
                stake_stg(private_key, from_chain, from_token, paths, file_path, i)

        if result:
            print_with_time(
                "  Произошла ошибка при выводе средств, либо при бридже Stargate | Завершаем работу с этим приватником...")
            print(f"          {get_address_wallet(private_key)}", flush=True)
            return


def task_wrapper(private_key):
    wallet = get_address_wallet(private_key)
    file_path = os.path.join("logs/paths", f"{wallet}.json")
    try:
        path_processing(private_key, file_path)
    except Exception as e:
        error_message = f"\n>>> Произошла ошибка: {str(e)}\n{type(e)}"
        print(error_message)
    time.sleep(random.uniform(delay_wallets[0], delay_wallets[1]))


def ask_overwrite(wallets_list, wallet_address_map, aptos_address_map):
    paths_folder = "logs/paths"

    if not os.path.exists(paths_folder):
        os.makedirs(paths_folder)

    existing_paths = 0
    missing_paths = 0
    existing_wallets = []

    for wallet in wallets_list:
        file_path = os.path.join(paths_folder, f"{wallet}.json")
        if os.path.exists(file_path):
            existing_paths += 1
            existing_wallets.append(wallet)
        else:
            missing_paths += 1

    print_with_time(f"  {existing_paths}/{len(wallets_list)} кошельков уже имеют назначенный путь.")

    wallets_to_generate = []

    if existing_paths > 0:
        print(f"          Заменим пути для уже существующих? (yes/no)")
        answer = input(f"          ")
        if answer.lower() == "yes":
            wallets_to_generate.extend(existing_wallets)

    if missing_paths > 0:
        wallets_to_generate.extend([wallet for wallet in wallets_list if wallet not in existing_wallets])

    generated_paths_count = 0

    for wallet in wallets_to_generate:
        file_path = os.path.join(paths_folder, f"{wallet}.json")
        path = generate()
        withdrawal_address = wallet_address_map.get(wallet)
        aptos_address = aptos_address_map.get(wallet)
        for el in path:
            if el["type"] == "WITHDRAWAL" and el.get("project") == "amount out":
                el["address"] = withdrawal_address
            if el["type"] == "Bridge" and el.get("project") == "Aptos":
                el["address"] = aptos_address
        with open(file_path, 'w') as f:
            json.dump(path, f)

        generated_paths_count += 1

    if generated_paths_count > 0:
        paths_spreadsheet(wallets_list)
        print_with_time(f"  Сгенерировано {generated_paths_count} путей. Начинаем работать? (yes/no):")
        proceed = input(f"          ")
        if proceed.lower() == "yes":
            return True
        else:
            return False
    return True


def main():
    private_keys = load_wallets()
    wallets_list = [get_address_wallet(pk) for pk in private_keys]
    print(f'Кол-во потоков: {number_of_threads}')
    print(f'Кол-во кошельков: {len(wallets_list)}')

    if withdrawal_out:
        wallet_address_map = load_withdrawal_addresses(wallets_list)
        print(f'Кол-во withdraw-адресов: {len(wallet_address_map)}')
    else:
        wallet_address_map = {}
        print(f'Вы выключили депозит обратно на биржу')

    if use_aptos:
        aptos_address_map = load_aptos_addresses(wallets_list)
        print(f'Кол-во aptos-адресов: {len(aptos_address_map)}\n')
    else:
        aptos_address_map = {}

    if not check_all_rpcs():
        print_with_time('  Один или более RPC не работают. Проверьте список RPC и повторите попытку.')
        return

    time.sleep(2)
    proceed = ask_overwrite(wallets_list, wallet_address_map, aptos_address_map)
    if not proceed:
        print("Завершаем работу скрипта, так как ответ не содержал: yes...\n")
        return

    okx_transfer()

    with ThreadPoolExecutor(max_workers=number_of_threads) as executor:
        for private_key in private_keys:
            delayTh(delay_threads)
            executor.submit(task_wrapper, private_key)

    okx_transfer()


if __name__ == '__main__':
    main()
