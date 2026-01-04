import os
from dotenv import load_dotenv
import sys
import logging
from Lucca import Lucca

if __name__ == "__main__":
    # init logging
    logging.basicConfig(filename='api.log', level=logging.INFO,  format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
    logging.info("Initialisation")
    
    # load env
    load_dotenv()
    api_key = os.getenv("API_KEY")
    base_url = os.getenv("BASE_URL")

    # init lucca
    lucca = Lucca(base_url, api_key)
    functions = {
        'all_sequential': lucca.fetch_store_all, 
        'all': lucca.fetch_store_all_parallel, 
        'users': lucca.fetch_store_users, 
        'contracts': lucca.fetch_store_contracts, 
        'departments' : lucca.fetch_store_departments,

    }

    if(len(sys.argv) > 1 and sys.argv[1] in functions.keys()):
        functions[sys.argv[1]]()
    else:
        print("\nInvalid command, enter one of the following prompt to fetch data:\n" \
        "\npython Lucca/main.py users -> fetch users data" \
        "\npython Lucca/main.py contracts -> fetch contract data" \
        "\npython Lucca/main.py departments -> fetch departments data")