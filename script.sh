#!/bin/bash

#exporting the airflow home variable
export AIRFLOW_HOME=$(pwd)

#installing the required libs
pip install -r requirements.txt -q --no-cache
pip install pandas
pip install pytest

#migrating the airflow metastore database
echo "######################################################################################"
echo -e "######################################################################################\n"
echo -e "migrating the airflow metastore database\n"
airflow db migrate

#creating users if required
echo "######################################################################################"
echo -e "######################################################################################\n"
echo -e "creating the airflow user\n"
# Prompt the user and read input in a single line
read -p "Do you want to create a user? enter your choice (yes/no): " user_input

# Check if the user input is equal to "yes"
if [ "$user_input" == "yes" ]; then
    read -p "Enter the user_name: " username
    read -p "Enter the firstname: " firstname
    read -p "Enter the lastname: " lastname
    read -p "Enter the email: " email

    airflow users create \
        --username $username \
        --firstname $firstname \
        --lastname $lastname \
        --role Admin \
        --email $email
    
elif [ "$user_input" == "no" ]; then
    echo "You chose 'no'."
else
    echo "choice is not valid"
fi

#run the webserver in the detached mode
echo "######################################################################################"
echo -e "######################################################################################\n"
echo -e "Run the webserver in detached mode\n"
airflow webserver -D
#run the scheduler in the detached mode
echo "######################################################################################"
echo -e "######################################################################################\n"
echo -e "Run the scheduler in detached mode\n"
airflow scheduler -D

#create the default connections to create sqlite_default connection
echo "######################################################################################"
echo -e "######################################################################################\n"
echo -e "Create the default connections\n"
airflow connections create-default-connections

#testing each task separately
echo "######################################################################################"
echo -e "######################################################################################\n"
echo -e "test the whole dag\n"
airflow dags list
echo "######################################################################################"
echo -e "######################################################################################\n"
airflow dags test etl_dag
