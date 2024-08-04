# Deel // Home Task

This is a project that aims to fetch data from the New Yout Times - Books API

All the tables are already there, but if you'd like to run the project from scratch please follow the bellow instructions:

To run this project from a container please make sure you have docker daemon running and from the root of this repo, type the following command </br>
```
docker build . -t nyt_books:test
docker run -it --entrypoint nyt_books:test
```

Now you should be inside the container</br>

Please do your local_setup: </br>
```
source ./scripts/local_setup
dbt debug
```
-> You should get a:</br> 
`Connection test: [OK connection ok]`

Now run the project:
```
./scripts/run_project
```

-> This should take about 1 hour, as the api has a restriction of 5 requests per minute and for the dbt project to run
