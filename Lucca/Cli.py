import os
from dotenv import load_dotenv
import sys
import logging
from Lucca2 import Lucca

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
        'all_sequential': lucca.export_all, 
        'all': lucca.export_all_parallel, 
        'users': lucca.export_users, 
        'contracts': lucca.export_contracts, 
        'departments' : lucca.export_departments,

    }

    if(len(sys.argv) > 1 and sys.argv[1] in functions.keys()):
        functions[sys.argv[1]]()
    else:
        print("\nInvalid command, enter one of the following prompt to fetch data:\n" \
        "\npython Lucca/Cli.py all -> recuperation de toutes les données" \
        "\npython Lucca/Cli.py all_sequential -> recuperation de toutes les données de maniere sequentielle" \
        "\npython Lucca/Cli.py users -> recuperation des données salariés" \
        "\npython Lucca/Cli.py contracts -> recuperation des données de contrats" \
        "\npython Lucca/Cli.py departments -> recuperation des données de departements")