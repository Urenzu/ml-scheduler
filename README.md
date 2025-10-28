# Build image
docker build -t ml_scheduling .

# Run container
docker run --rm -it ml_scheduling
or
docker run --rm -it -v ${PWD}\data:/app/data ml_scheduling


