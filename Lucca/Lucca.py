import os
from dotenv import load_dotenv
import requests
import logging
import datetime
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from concurrent.futures import ThreadPoolExecutor,  as_completed


class Lucca:
    def __init__(self, base_url, api_key, output_base_path="Results"):

        # load arguments
        self.base_url = base_url
        self.key = api_key
        self.headers = {
            "Authorization": f"lucca application={self.key}"
        }

        # setup request adapter and retry strategy
        retry = Retry(
            total=3,
            status_forcelist=[425, 429, 500, 502, 503, 504, 507],
            allowed_methods=['GET'],
            backoff_factor=1.1,
            raise_on_status=False,
            # si la reponse est 429 -> attendre le montant indiqué
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry)
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

        # Create output folder
        current_day = datetime.datetime.now().strftime("%Y-%m-%d")
        self.output_path = os.path.join(output_base_path, current_day)
        os.makedirs(self.output_path, exist_ok=True)

    # Fetch data

    def fetch_api(self, end_point, params=None):

        query = self.base_url + end_point

        if params is None:
            params = {}

        try:
            response = self.http.get(query, headers=self.headers, params=params, timeout=(
                15, 90))  # read, write timeout
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur lors de la requete {end_point} :\n {e}")
            raise e

        return response

    def iter_offset_fetch(self, end_point, base_params, limit=100, offset=0,  data_path=('data', 'items')):

        params = dict(base_params)

        if 'paging' not in params.keys():
            params['paging'] = f"{offset},{limit}"

        offset = int(params['paging'].split(",")[0])
        limit = int(params["paging"].split(',')[1])

        while True:
            logging.info(f'fetch {end_point}: {offset}->{limit} ')
            response = self.fetch_api(end_point, params).json()

            if not response:
                logging.error(f'Reponse vide a {end_point}')
                break

            # On deballe le json selon le chemin e argument
            if data_path:
                for path in data_path:
                    response = response[path]

            yield response

            if len(response) < limit:
                logging.info(f"Tous les elements de {end_point} ont ete charge")
                break

            offset += limit

            params['paging'] = f"{offset},{limit}"

    # Appel de l'api en plusieurs iteration en suivant le format page et limit.
    # soit on passe page et limit dans params soit directement dans les parametres de la fonction. En cas d'oublie la fonction fournis des valeurs par defaut
    def iter_page_fetch(self, end_point, base_params, limit=100, page=1, data_path=('items',)):
        params = dict(base_params)

        if 'limit' not in params.keys():
            params['limit'] = limit

        if 'page' not in params.keys():
            params['page'] = page

        while True:

            logging.info(f"fetch {end_point}: page: {params['page']} elements {params['limit'] * params['page'] } -> {params['limit'] * (params['page'] + 1) }")
            response = self.fetch_api(end_point, params).json()

            if not response:
                logging.error(f'Reponse vide a {end_point}')

            if data_path:
                for path in data_path:
                    response = response[path]


            yield response

            if len(response) < int(params['limit']):
                logging.info(f"Tous les elements de {end_point} ont ete charge")
                break

            params['page'] = int(params['page']) + 1

    def store_to_csv(self, data, csv_name="data", columns=None, reset=True):

        csv_path = os.path.join(self.output_path, csv_name)

        df = pd.json_normalize(data)
        
        if not columns:
            columns = list(df.columns)

        df = df.reindex(columns=columns)

        df.to_csv(csv_path, index=False,
                  mode="w" if reset else "a", header=reset)

        return csv_path

    # get users data
    def fetch_store_users(self):

        # end_point api
        end_point = "/api/v3/users"

        # params
        limit = 100  # limit de resultat par iteration
        fields = ['id', 'firstName', 'lastName', 'mail', "jobTitle", "gender",
                  "legalEntityId", "departmentId", "managerId", "seniorityDate", "directLine"]
        params = {
            'formerEmployees': 'true',
            'fields': ",".join(fields),
        }

        for i, page in enumerate(self.iter_offset_fetch(end_point, base_params=params, limit=limit)):  # fetch
            # write
            # la premiere execution on reset le csv, les autres on append juste les données
            path = self.store_to_csv(page, csv_name='users.csv',
                              columns=fields, reset=(i == 0))
        print(f"CSV genere a: {path} ")

    # Fetch departments data

    def fetch_store_departments(self):

        #end point api
        end_point = "/api/v3/departments"

        # params
        limit = 100
        fields = ["id", "name", "code", "hierarchy", "parentId",
                  "isActive", "position", "level", "currentUsersCount", "headId"]
        
        params = {'fields': ",".join(fields)}

        for i, page in enumerate(self.iter_offset_fetch(end_point, base_params=params, limit=limit)):
            path = self.store_to_csv(page, csv_name='departments.csv',
                              columns=fields, reset=(i == 0))
        print(f"CSV genere a: {path} ")

    # get contracts data

    def fetch_store_contracts(self):

        fields = ["id", "ownerId", "isApplicable", "internshipSupervisorId", "externalId", "startsOn",  "endsOn", "trialPeriodDays",
                  "renewedTrialPeriodDays", "trialPeriodEndDate", "trialPeriodEndDate2", "terminationReasonId",   "authorId"]
        params = {
            'fields': ",".join(fields),
            'page': 1,
            'limit': 100
        }
        end_point = "/directory/api/4.0/work-contracts"

        for i, page in enumerate( self.iter_page_fetch( end_point, base_params=params)):
            path = self.store_to_csv(page, csv_name='contracts.csv',
                              columns=fields, reset=(i == 0))
        print(f"CSV genere a: {path} ")


    # Execution des differents fetch de maniere sequentielle
    def fetch_store_all(self):
        tasks = [self.fetch_store_users, self.fetch_store_contracts,
                 self.fetch_store_departments]
        [task() for task in tasks]

    # Execution en parallele des requetes pour gagner du temps -> l'API renvoie des status 429 apres >50 requetes par minutes
    def fetch_store_all_parallel(self):
        tasks = [self.fetch_store_users, self.fetch_store_contracts,
                 self.fetch_store_departments]
        with ThreadPoolExecutor(max_workers=3) as ex:
            [ex.submit(task) for task in tasks]
                

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
    lucca.fetch_store_all_parallel()

