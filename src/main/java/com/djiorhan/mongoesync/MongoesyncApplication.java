package com.djiorhan.mongoesync;

import org.springframework.boot.SpringApplication;

import java.io.IOException;
import java.util.ArrayList;
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

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
		document.forEach((key, value) -> {
			if (value instanceof org.bson.types.ObjectId) {
				if (!key.equals("_id")) {
					map.put(key, value.toString());
				}
			} else {
				map.put(key, value);
			}
		});
		return map;
	}

	private void indexDocumentToElasticsearch(RestHighLevelClient client, Document document) throws IOException {
		IndexRequest request = new IndexRequest("spring-boot-2")
				.id(document.getObjectId("_id").toString())
				.source(documentToMap(document));
		IndexResponse response = client.index(request, RequestOptions.DEFAULT);
	}

	private void executeCustomMethod() throws Exception {
		try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
			RestHighLevelClient elasticClient = new RestHighLevelClient(
					RestClient.builder(new org.apache.http.HttpHost(elasticsearchHost, 9200, "http")));
			MongoDatabase database = mongoClient.getDatabase(mongoDatabase);
			MongoCollection<Document> collection = database.getCollection("documents");

			long startTime = System.currentTimeMillis();

			ExecutorService executor = Executors.newFixedThreadPool(10);
			List<CompletableFuture<Void>> futures = new ArrayList<>();

			try (MongoCursor<Document> cursor = collection.find().iterator()) {
				while (cursor.hasNext()) {
					Document document = cursor.next();
					CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
						try {
							indexDocumentToElasticsearch(elasticClient, document);
						} catch (IOException | ElasticsearchStatusException e) {
							System.err.println("Error indexing document ID: " + document.getObjectId("_id"));
							e.printStackTrace();
						}
					}, executor);
					futures.add(future);
				}
			}

			CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.MINUTES);

			long endTime = System.currentTimeMillis();
			long duration = endTime - startTime;
			System.out.println("Execution time: " + duration + " ms");

			elasticClient.close();
		}
	}

}
