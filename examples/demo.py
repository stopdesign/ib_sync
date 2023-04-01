from time import sleep

from ib_sync import IBSync, IBThread


def main(ib: IBSync):
    sid = "ARCA_URA"

    contract = ib.contract_for_sid(sid)

    cd = ib.get_contract_details(contract)
    details = cd[0]
    contract = details.contract

    print(details.__dict__)
    print(contract.__dict__)


if __name__ == "__main__":
    ib = IBSync()
    ib.connect("10.0.10.1", 4001, clientId=299)

    try:
        IBThread(ib).start()
        sleep(1)
        main(ib)
    except KeyboardInterrupt:
        print()
        print("DONE")
    finally:
        ib.disconnect()
