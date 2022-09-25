
Sparks Networks - Junior Data Engineer task

Techstack used - 
    * Python/Pandas (scripting / Programming)
    * Airflow (orchestrator)
    * Postgresql (database) Datawarehouse
    * Bigquery Datawarehouse
    * Docker containers
    * git (code/version management)

  See dataflow.png for the High level design of the project.

  For the PII info we have used Column level access so that only those info that are required by the final users are provided , they are restricted of any other columns. See file  "sparks.sql" for complete info about the tables , roles and other object.

  We have dags folder having the sparkdag.py file which is responsible for the ETL process.

  
  Steps to generate and run the DAG.

  1>  clone the git  using 
        git clone https://github.com/Villain1401036/Sparks.git
  
  2> install docker and docker compose  
       use this link for more info 
       https://docs.docker.com/engine/install/ubuntu/

  3>  go into the directory /Sparks and run docker compose up 
      
      cd Sparks
      docker compose up 

  4>  run the postgresinstall.sh in Sparks folder /Sparks/postgresinstall.sh
      
      sh postgresinstall.sh
      
      This will install the postgres and create the tables in the postgres database 
  
  5>  open airflow webserver UI using  <ip address>:8080  eg - 192.168.4.21:8080

  6>  open connections and add GCP connection as 
       *Connection id - gcp_conn_default
       *connection type - Google Cloud
       *Project Id - sparks-363212
       *keyfileJson - ( Due to its privacy Please ask me on kr7168799@gmail.com for setup  ) 

  7>  Run the Sparks DAG in the DAGs in Airflow UI 


  ** Project was made on a Virtual Machine (using Ubuntu as the unix system ) 
     for different system there may be some changes .
     








