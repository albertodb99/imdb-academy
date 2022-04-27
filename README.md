# imdb-academy
Repository for the Empathy's Academy program consisting on an IMDB like search engine using Elasticsearch and Springboot

## Requirements ##
- ElasticSearch 7.17.2
- Docker
- Java 17
- SpringBoot 2.6.6
- (Optional)Some application to do requests easily, in my case, I use Insomnia

## Installation ##
First of all, you have to download the project: 
```
git clone https://github.com/uo266536/imdb-academy.git
```
&nbsp;

Once you have the code downloaded, go ahead the folder, and start the docker container (make sure you have Docker downloaded):
```
cd imdb
docker-compose up
```
&nbsp;

Now, you are able to compile and start running the project with the following commands:
```
mvn compile
mvn spring-boot:run
```
&nbsp;

If everything has been done correctly, at this point, you should have the application running perfectly.

## First steps ##
What you have to do first, is to create the index films, by running the following PUT Request:
```
localhost:8080/films
```
&nbsp;

Now that you have the index created, it is the time to index the documents, by running the following GET Request:
```
localhost:8080/index_documents
```
It is very important that in the query you add two variables: filmsPath which contains the path of the .tsv file corresponding to title.basics.tsv, which is the file expected; and ratingsPathOpt, which is optional, and contains the path of the .tsv file corresponding to the title.ratings.tsv file.

&nbsp;

This operations can be done easily looking to the Swagger Documentation provided, just in case you struggle doing it with any application, opening the following URL when the application is running: http://localhost:8080/swagger-ui/index.html
