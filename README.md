# imdb-academy
Repository for the Empathy's Academy program consisting on an IMDB like search engine using Elasticsearch and Springboot

## Requirements ##
- Maven 3.8.4
- Docker
- Java 17
- SpringBoot 2.6.6
- Datasets that can be downloaded from here: https://drive.google.com/drive/folders/1pv2cRuO-g0CRzVxN9LY9yrpI6pWj12rI?usp=sharing
- (Optional) Some application to do requests easily, in my case, I use Insomnia
- (Optional) You can use SDKMan to download Maven, Java and Springboot easily!

## Installation ##
First of all, you have to download the project: 
```
git clone https://github.com/uo266536/imdb-academy.git
```
&nbsp;

Once you have the code downloaded, go ahead the folder, and start the docker container (make sure you have Docker downloaded):
```
cd imdb
docker-compose up --build -d
```
&nbsp;

Once you do this, you will have running both docker container and the API. This has only to be done the first time.
After that, with this command you should be able to start running both containers:
```
docker-compose up -d
```
&nbsp;

If everything has been done correctly, at this point, you should have the application running perfectly.

## First steps ##
What you have to do first, is to create the index films, by running the following PUT Request:
```
localhost:8080/films OR 
curl --request PUT \ --url http://localhost:8080/films
```
&nbsp;

Now that you have the index created, it is the time to index the documents, by running the following GET Request:
```
localhost:8080/index_documents OR
curl --request POST \ --url 'http://localhost:8080/index_documents
```
It is very important that in the query you add all the datasets included in the Drive folder posted before. If not, it will not work, as it needs all of them

Example:
&nbsp;

<img width="510" alt="image" src="https://user-images.githubusercontent.com/60233035/165483860-1de4928b-002b-431c-83ee-4c946b16c775.png">

&nbsp;

Once you have done this, you are able to start doing queries ;)

This operations can be done easily looking to the Swagger Documentation provided, just in case you struggle doing it with any application, opening the following URL when the application is running: http://localhost:8080/swagger-ui/index.html
