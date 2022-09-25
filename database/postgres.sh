sudo apt-get update 
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql.service
sudo su postgres
psql -U postgres -p5433 -a -q -f /home/$USER/postgresinit.sql








