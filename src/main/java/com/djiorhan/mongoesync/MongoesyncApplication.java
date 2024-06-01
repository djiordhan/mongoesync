package com.djiorhan.mongoesync;

import org.springframework.boot.SpringApplication;

import java.io.IOException;
import java.util.HashMap;

import org.bson.Document;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Map;

@SpringBootApplication
public class MongoesyncApplication implements CommandLineRunner {

	@Value("${elasticsearch.host}")
	private String elasticsearchHost;
	@Value("${mongodb.uri}")
	private String mongoUri;
	@Value("${mongodb.database}")
	private String mongoDatabase;

	public static void main(String[] args) {
		SpringApplication.run(MongoesyncApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		executeCustomMethod();
	}

	private Map<String, Object> documentToMap(Document document) {
		Map<String, Object> map = new HashMap<>();
		document.forEach(map::put);
		return map;
	}

	private void executeCustomMethod() throws IOException {
		try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
			RestHighLevelClient elasticClient = new RestHighLevelClient(RestClient.builder(new org.apache.http.HttpHost(elasticsearchHost, 9200, "http")));
			MongoDatabase database = mongoClient.getDatabase(mongoDatabase);
			MongoCollection<Document> collection = database.getCollection("documents");

			try (MongoCursor<Document> cursor = collection.find().iterator()) {
				while (cursor.hasNext()) {
					Document document = cursor.next();
					IndexRequest request = new IndexRequest("spring-boot-test")
							.id(document.getObjectId("_id").toString())
							.source(documentToMap(document));

					try {
						elasticClient.index(request, RequestOptions.DEFAULT);
						System.out.println("Done for document: " + document.getObjectId("_id"));
					} catch (IOException e) {
						System.out.println("Error for document: " + document.getObjectId("_id"));
					}
				}
			}

			elasticClient.close();
		}
	}

}
