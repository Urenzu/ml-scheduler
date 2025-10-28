# Primary build:
docker-compose up --build

# Primary shutdown
docker-compose down
docker volume rm ml_scheduling_postgres_data

docker-compose down -v (For removing containers, network, and named volumesn like postgres_data)

# Run and check DB:
docker-compose up -d postgres
docker-compose ps (To see image)

# Check DB:
docker exec -it ml_scheduler_db psql -U ml_user -d ml_scheduler
\dt (To open)
\q (To close)