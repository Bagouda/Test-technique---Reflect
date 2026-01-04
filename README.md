# Solution au test technique
  
Voici ma solution à l'exercice proposé  
  
## Étapes de configuration  
  
La solution nécessite un fichier .env à la racine du projet avec les paramètres suivants:  
  
API_KEY={clé de l'API}  
BASE_URL={url de l'API}  
  
Le projet utilise également des modules externes qui sont indiqués dans le fichier "requirements.txt". Il suffit alors d'exécuter la commande suivante pour installer les modules aux bonnes versions:  
  
pip install -r requirements.txt  
  
## Structure du projet  
  
.  
├── Lucca/  
│   ├── Lucca.py          # Classe permettant d'effectuer les appels à l'API et de stocker les données en CSV  
│   └── cli.py            # Commandes pour utiliser les fonctions de Lucca.py depuis le terminal  
│  
├── Results/              # Données extraites de l'API  
│   └── [YYYY]-[MM]-[DD]/ # Extractions enregistrées dans un dossier associé à leur date d'exécution  
│       ├── contracts.csv    # Informations sur les contrats de travail de l'entreprise  
│       ├── departments.csv  # Informations sur les départements de l'entreprise  
│       └── users.csv        # Informations sur les salariés  
│  
├── .env                 # Variables d'environnement pour utiliser l'API  
├── api.log              # Log daté de l'exécution du programme  
└── requirements.txt     # Liste des modules Python à installer (versions incluses)  
  

  
## Utilisation  
  
Il y a deux manières d'utiliser le projet:  
  
1) Utilisation générale pour remplir tous les csv. Il suffit d’exécuter le fichier Lucca.py depuis le terminal:  
        python Lucca/Lucca.py   

2) Utilisation spécifique pour remplir les csv souhaités. Les commandes suivantes sont possibles dans le terminal:  

    python Lucca/Cli.py all  
    python Lucca/Cli.py all_sequential  
    python Lucca/Cli.py users  
    python Lucca/Cli.py contracts  
    python Lucca/Cli.py departments  
  
  
### Explication des commandes  
  
- python Lucca/Cli.py all -> exécute les requêtes en parallèle et remplit trois csv (équivalent à exécuter: python Lucca/Lucca.py)  
  
- python Lucca/Cli.py all_sequential -> exécute les requêtes de manière séquentielle et remplit les trois csv.  
  
- python Lucca/Cli.py users -> exécute les requêtes uniquement pour remplir le csv sur les salariés.  
  
- python Lucca/Cli.py contracts -> exécute les requêtes uniquement pour remplir le csv sur les contrats de travail des salariés.  
  
- python Lucca/Cli.py departments -> exécute les requêtes uniquement pour remplir le csv sur les départements de l'entreprise.  
