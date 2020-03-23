import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class Test {

    private static final String REGEX = "(?<name>.+)\\,(?<age>\\d+)\\,(?<courses>.+)";
    private static final Pattern PATTERN = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
    private static String csvFile = "src/main/resources/mongo.csv";

    public static void main(String[] args) throws IOException {
        MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );
        MongoDatabase database = mongoClient.getDatabase("local");

        // Создаем и очищаем коллекцию
        MongoCollection<Document> collection = database.getCollection("Students");
        collection.drop();

        // Парсим csv-файл
        List<String> csvLines = Files.readAllLines(Paths.get(csvFile));

        // Добавляем каждую запись в коллекцию БД
        csvLines.forEach(line -> collection.insertOne(lineToDocument(line)));

        System.out.println("Всего студентов: " + collection.countDocuments());

        BsonDocument query = BsonDocument.parse("{Age: {$gt: 40}}");
        System.out.println("Кол-во студентов старше 40 лет: " + collection.countDocuments(query));

        query = BsonDocument.parse("{Age: 1}");
        collection.find().sort(query).limit(1).forEach((Consumer<Document>) user ->
                System.out.println("Имя самого молодого студента: " + user.getString("Name")));

        query = BsonDocument.parse("{Age: -1}");
        collection.find().sort(query).limit(1).forEach((Consumer<Document>) user ->
                System.out.println("Список курсов самого старого студента: " + user.getString("Courses")));
    }

    private static Document lineToDocument(String line) {
        var matcher = PATTERN.matcher(line);
        if (matcher.find()) {
            return  new Document()
                    .append("Name", matcher.group("name"))
                    .append("Age", Integer.parseInt(matcher.group("age")))
                    .append("Courses", matcher.group("courses"));
        }
        return null;
    }

    private static void printCollection(MongoCollection<Document> collection) {
        collection.find().forEach((Consumer<Document>) doc -> System.out.println(doc));
    }
}
