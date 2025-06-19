package com.mycompany.app;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.InsertOneOptions;
import org.bson.Document;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.model.Updates;
import org.bson.json.JsonWriterSettings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import java.util.HashSet;
import java.util.Set;

public class App {
	String PORT_MASTER = "172.31.150.163:8888"; // IP_MASTER:PORT
	String PORT_SLAVE = "172.31.156.32:7777"; // IP_SLAVE:PORT 
	String uri = "mongodb://" + PORT_MASTER + "," + PORT_SLAVE;
	String dbName = "jurnal_galeri"; // nama database;
	String collName = "isi_jurnal"; // nama collection/tab
	
    List<String> headers = new ArrayList<>();
	Scanner scanner = new Scanner(System.in);
	JsonWriterSettings prettyPrint = JsonWriterSettings.builder()
            .indent(true)
            .build();

	private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ';' && !inQuotes) {
                fields.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        fields.add(current.toString().trim());
        return fields;
    }

	public void insertFile(String filename) {
        String csvFile = filename;

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collName);

			if(collection.estimatedDocumentCount() > 0) {
				Set<String> allKeys = new HashSet<>();

				System.out.println("\n----------------------------------------------------");
				System.out.println("Load all headers key");
				System.out.println("-----------------------------------------------------\n");
				
				try (MongoCursor<Document> cursor = collection.find().limit(1).iterator()) {
					while (cursor.hasNext()) {
						Document doc = cursor.next();
						Set<String> orderedKeys = doc.keySet();
						orderedKeys.forEach(e -> {
							if(!e.equals("_id")) headers.add(e);
						});
					}
				}

				return;
			}

			System.out.println("\n----------------------------------------------------");
			System.out.println("First initiate " + filename + " to mongodb server");
			System.out.println("-----------------------------------------------------\n");

            try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
                String line;
                int lineCount = 0;

                while ((line = br.readLine()) != null) {
                    List<String> fields = parseCsvLine(line);

                    if (lineCount == 0) {
                        for (String header : fields) {
                            headers.add(header.replace(".", "_"));                        
						}
                    } else {
                        Document doc = new Document();
                        for (int i = 0; i < Math.min(fields.size(), headers.size()); i++) {
                            doc.append(headers.get(i), fields.get(i));
                        }

                        try {
                            collection.insertOne(doc, new InsertOneOptions());
                        } catch (MongoException me) {
                            System.err.println("Insert failed: " + me.getMessage());
                        }
                    }

                    lineCount++;
                }

                System.out.println("Finished inserting " + (lineCount - 1) + " documents.");

            } catch (IOException e) {
                System.err.println("File error: " + e.getMessage());
            }
        }
    }

	public void findBy() {
		try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collName);

			System.out.println("\nSelect key to find");
			for (int i = 0; i < headers.size(); ++i) {
				System.out.println(String.valueOf(i + 1) + ". " + headers.get(i));
			}

			System.out.print("Select key using index (ex: 1 to find by Rank): ");
			int index = scanner.nextInt();
			scanner.nextLine();

			System.out.print("insert value to find: ");
			String value = scanner.nextLine();

			for(Document doc : collection.find(eq(headers.get(index - 1), value))) {
				System.out.println(doc.toJson(prettyPrint));
			}

			System.out.println();
        } catch (Exception e) {
			System.err.println("[ERROR]: " + e.getMessage());
		}
	}

	public void updateBy() {
		try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collName);

			System.out.println("\nSelect key to update by");
			for (int i = 0; i < headers.size(); ++i) {
				System.out.println(String.valueOf(i + 1) + ". " + headers.get(i));
			}

			System.out.print("Select key using index (ex: 1 to select by Rank): ");
			int indexSelect = scanner.nextInt();
			scanner.nextLine();

			System.out.print("insert value to find: ");
			String value = scanner.nextLine();

			System.out.print("Select what to update using key (ex: 2 to update by SourceId): ");
			int indexUpdate = scanner.nextInt();
			scanner.nextLine();

			System.out.print("insert value to update: ");
			String valueUpdate = scanner.nextLine();

			try {
				collection.updateOne(
					eq(headers.get(indexSelect - 1), value),
					Updates.set(headers.get(indexUpdate - 1), valueUpdate)
				);
				System.out.println("Updated data success");
            } catch (MongoException me) {
                System.err.println("Update failed: " + me.getMessage());
            }
			
        } catch (Exception e) {
			System.err.println("[ERROR]: " + e.getMessage());
		}
	}

	public void deleteBy() {
		try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collName);

			System.out.println("\nSelect key to delete by");
			for (int i = 0; i < headers.size(); ++i) {
				System.out.println(String.valueOf(i + 1) + ". " + headers.get(i));
			}

			System.out.print("Select key using index (ex: 1 to select by Rank): ");
			int indexSelect = scanner.nextInt();
			scanner.nextLine();

			System.out.print("insert value to find: ");
			String value = scanner.nextLine();

			try {
				collection.deleteOne(
					eq(headers.get(indexSelect - 1), value)
				);
				System.out.println("Delete data success");
            } catch (MongoException me) {
                System.err.println("Update failed: " + me.getMessage());
            }
			
        } catch (Exception e) {
			System.err.println("[ERROR]: " + e.getMessage());
		}
	}


	public void insert() {
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collName);

			Document doc = new Document();
			for (int i = 0; i < headers.size(); ++i) {
				System.out.print(String.valueOf(i + 1) + ". " + headers.get(i) + ": ");
				String val = scanner.nextLine();
				doc.append(headers.get(i), val);
			}

			try {
				collection.insertOne(doc, new InsertOneOptions());
				System.out.println("Insert data success");
            } catch (MongoException me) {
                System.err.println("Insert failed: " + me.getMessage());
            }
        } catch (Exception e) {
			System.err.println("[ERROR]: " + e.getMessage());
		}
    }

	public void cmdHeader() {
		System.out.println("\n---- Data Jurnal Management ----");
		System.out.println("command: ");
		System.out.println("1. insert new data");
		System.out.println("2. find data");
		System.out.println("3. update new data");
		System.out.println("4. delete data");
		System.out.println("q. quit");
		System.out.print("Enter Command: ");
	}

	public void interfaceTerminal() {
		cmdHeader();
		
		String cmd;
		while(!(cmd = scanner.nextLine()).equals("q")) {
			switch (cmd) {
				case "1":
					insert();
					cmdHeader();
					break;
				case "2":
					findBy();
					cmdHeader();
					break;
				case "3":
					updateBy();
					cmdHeader();
					break;
				case "4":
					deleteBy();
					cmdHeader();
					break;
				default:
					cmdHeader();
					break;
			}
		}


	}

    public static void main(String[] args) {
		App app = new App();
		app.insertFile("data_jurnal.csv");
		app.interfaceTerminal();

    }
}
// 172.31.107.158
