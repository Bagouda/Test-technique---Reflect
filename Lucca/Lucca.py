import os
from dotenv import load_dotenv
import requests
import logging
import datetime
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from concurrent.futures import ThreadPoolExecutor,  as_completed
from utils import json_path_extraction


class Lucca:
    def __init__(self, base_url, api_key, output_base_path="Results", fetch_limit = 100):

        # Chargement des arguments
        self.base_url = base_url
        self.key = api_key
        self.fetch_limit = fetch_limit


        # Init request
        self.headers = {
            "Authorization": f"lucca application={self.key}"
        }

        retry = Retry(
            total=3,
            status_forcelist=[425, 429, 500, 502, 503, 504, 507],
            allowed_methods=['GET'],
            backoff_factor=1.1,
            raise_on_status=False,  # si la reponse est 429 -> attendre le montant indiqué
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry)
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

        # Mise en place output
        current_day = datetime.datetime.now().strftime("%Y-%m-%d")
        self.output_path = os.path.join(output_base_path, current_day)
        os.makedirs(self.output_path, exist_ok=True)

    # Fetch data
    def fetch_api(self, end_point, params=None):

        query = self.base_url + end_point

        try:
            response = self.http.get(
                query, 
                headers=self.headers, 
                params=params if params is not None else {}, 
                timeout=( 15, 90) 
            )

            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur lors de la requete {end_point} :\n {e}")
            raise e

        return response.json()

    def paginate_endpoint(self, end_point, base_params, json_path=('data', 'items')):

        params = dict(base_params)

        # on recupere limit si il y a des arguments iteratif
        limit = 0
        if('limit' in params.keys()):
            limit = params['limit']
        elif('paging' in params.keys()):
            limit = int(params['paging'].split(',')[1])


        offset = 0

        while True:
            logging.info(f'fetch {end_point}: {offset}->{limit} ')

            load = self.fetch_api(end_point, params)

            if not load:
                logging.error(f'Reponse vide a {end_point}')
                break

            # On deballe le json selon le chemin e argument
            response = json_path_extraction(load, json_path)


            yield response

            
            if limit == 0 or len(response) < limit:
                logging.info(f"Tous les elements de {end_point} ont ete charge")
                break

            # On incremente suivant la methode d'extraction
            if('page' in params.keys()):
                params['page'] = int(params['page']) + 1
                print("page: ", params['page'])
            else:
                offset = int(params['paging'].split(",")[0]) + limit
                params['paging'] = f"{offset},{limit}"
                print("paging: ", params['paging'])
            

    def write_csv(self, data, csv_name="data", columns=None, reset=True):

        csv_path = os.path.join(self.output_path, csv_name)

        df = pd.json_normalize(data)
        
        if not columns:
            columns = list(df.columns)

        df = df.reindex(columns=columns)

        df.to_csv(csv_path, index=False, mode="w" if reset else "a", header=reset)

        return csv_path
    
    def fetch_store(self, end_point, params= {}, json_path=('data', 'items'),  csv_name="data.csv" ):

        for i, page in enumerate(self.paginate_endpoint(end_point, base_params=params, json_path= json_path)):

            path = self.write_csv(
                        page, 
                        columns=params['fields'].split(",") if 'fields' in params.keys() else None, 
                        csv_name=csv_name, 
                        reset=(i == 0)
                    )
            
        return path
    
    # get users data
    def export_users(self ):

        # end_point api
        end_point = "/api/v3/users"

        # params
        fields = ['id', 'firstName', 'lastName', 'mail', "jobTitle", "gender",
                  "legalEntityId", "departmentId", "managerId", "seniorityDate", "directLine"]
        params = {
            'formerEmployees': 'true',
            'fields': "extendedData," + ",".join(fields),
            'paging' : "0,100" # offset,limit
        }

        path = self.fetch_store(end_point,params, csv_name="users.csv" )
        print(f"CSV genere a: {path} ")

    # get departments data
    def export_departments(self):

        #end point api
        end_point = "/api/v3/departments"

        # params
        fields = ["id", "name", "code", "hierarchy", "parentId",
                  "isActive", "position", "level", "currentUsersCount", "headId"]
        
        params = {
            'fields': ",".join(fields),
            'paging' : "0,100" # offset,limit
        }


        path = self.fetch_store(end_point,params, csv_name="departments.csv" )
        print(f"CSV genere a: {path} ")

    # get contracts data
    def export_contracts(self):

        fields = ["id", "ownerId", "isApplicable", "internshipSupervisorId", "externalId", "startsOn",  "endsOn", "trialPeriodDays",
                  "renewedTrialPeriodDays", "trialPeriodEndDate", "trialPeriodEndDate2", "terminationReasonId",   "authorId"]
        params = {
            'fields': ",".join(fields),
            'page': 1,
            'limit': 100
        }
        end_point = "/directory/api/4.0/work-contracts"
        
        path = self.fetch_store(end_point,params, csv_name='contracts.csv', json_path=('items',) )
        print(f"CSV genere a: {path} ")


    # Execution des differents fetch de maniere sequentielle
    def export_all(self):
        tasks = [
            self.export_users, 
            self.export_contracts,
            self.export_departments
        ]
        
        [task() for task in tasks]

    # Execution en parallele des requetes pour gagner du temps -> l'API renvoie des status 429 apres >50 requetes par minutes
    def export_all_parallel(self):
        tasks = [
            self.export_users, 
            self.export_contracts,
            self.export_departments
        ]
        
        with ThreadPoolExecutor(max_workers=min(len(tasks), 10)) as ex:
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
    fetch_limit = int(os.getenv("Limit") )

    # init lucca
    lucca = Lucca(base_url, api_key, fetch_limit= fetch_limit)
    lucca.export_all_parallel()