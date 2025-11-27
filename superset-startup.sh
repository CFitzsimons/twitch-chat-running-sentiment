docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin
docker exec -it superset superset init