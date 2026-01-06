
def json_path_extraction(response, json_path):

    try:
        if json_path:
            for path in json_path:
                response = response[path]
    except Exception as e:
        print("Erreur lors de l'extraction de données dans la reponse, verifier le format.\n", e)
        raise(e)
    
    return response