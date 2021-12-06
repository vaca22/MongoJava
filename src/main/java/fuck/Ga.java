package fuck;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class Ga {

    private static MongoClient mongoClient;
    private static MongoDatabase mongoDatabase;

    public static void main(String[] args) {
        try{
            // 连接到 mongodb 服务
            mongoClient = new MongoClient( "localhost" , 27017 );

            // 连接到数据库
            mongoDatabase = mongoClient.getDatabase("wsmslgh");
            System.out.println("Connect to database successfully");


            mongoDatabase.createCollection("fuck");
            System.out.println("集合创建成功");

        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }


    public static MongoCollection<Document> getCollection(String dbName, String collName) {
        if (null == collName || "".equals(collName)) {
            return null;
        }
        if (null == dbName || "".equals(dbName)) {
            return null;
        }
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collName);
        return collection;
    }

    public static MongoDatabase getDB(String dbName) {
        if (dbName != null && !"".equals(dbName)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            return database;
        }
        return null;
    }


    public void getAllDBNames(){
        MongoIterable<String> dbNames = mongoClient.listDatabaseNames();
        for (String s : dbNames) {
            System.out.println(s);
        }
    }

    public void getAllCollections(){
        MongoIterable<String> colls = getDB("books").listCollectionNames();
        for (String s : colls) {
            System.out.println(s);
        }
    }


    public void dropDB(){
        //连接到数据库
        MongoDatabase mongoDatabase =  getDB("test");
        mongoDatabase.drop();
    }


    public void insertOneTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //要插入的数据
        Document document = new Document("id",1)
                .append("name", "哈姆雷特")
                .append("price", 67);
        //插入一个文档
        collection.insertOne(document);
        System.out.println(document.get("_id"));
    }

    public void insertManyTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //要插入的数据
        List<Document> list = new ArrayList<>();
        for(int i = 1; i <= 15; i++) {
            Document document = new Document("id",i)
                    .append("name", "book"+i)
                    .append("price", 20+i);
            list.add(document);
        }
        //插入多个文档
        collection.insertMany(list);
    }


    public void findAllTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //查询集合的所有文档
        FindIterable findIterable= collection.find();
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public void findConditionTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //方法1.构建BasicDBObject  查询条件 id大于2，小于5
        BasicDBObject queryCondition=new BasicDBObject();
        queryCondition.put("id", new BasicDBObject("$gt", 2));
        queryCondition.put("id", new BasicDBObject("$lt", 5));
        //查询集合的所有文  通过price升序排序
        FindIterable findIterable= collection.find(queryCondition).sort(new BasicDBObject("price",1));

        //方法2.通过过滤器Filters，Filters提供了一系列查询条件的静态方法   id大于2小于5   通过id升序排序查询
        //Bson filter=Filters.and(Filters.gt("id", 2),Filters.lt("id", 5));
        //FindIterable findIterable= collection.find(filter).sort(Sorts.orderBy(Sorts.ascending("id")));

        //查询集合的所有文
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }


    public void findAllTest3(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
//查询id等于1,2,3,4的文档
        Bson fileter= Filters.in("id",1,2,3,4);
        //查询集合的所有文档
        FindIterable findIterable= collection.find(fileter).projection(new BasicDBObject("id",1).append("name",1).append("_id",0));
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }



    public void getCountTest() {
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //获取集合的文档数
        Bson filter = Filters.gt("price", 30);
        int count = (int)collection.count(filter);
        System.out.println("价钱大于30的count==："+count);
    }

    public void findByPageTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //分页查询  跳过0条，返回前10条
        FindIterable findIterable= collection.find().skip(0).limit(10);
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        System.out.println("----------取出查询到的第一个文档-----------------");
        //取出查询到的第一个文档
        Document document = (Document) findIterable.first();
        //打印输出
        System.out.println(document);
    }

    public void updateTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //修改id=2的文档    通过过滤器Filters，Filters提供了一系列查询条件的静态方法
        Bson filter = Filters.eq("id", 2);
        //指定修改的更新文档
        Document document = new Document("$set", new Document("price", 44));
        //修改单个文档
        collection.updateOne(filter, document);
        //修改多个文档
        // collection.updateMany(filter, document);
        //修改全部文档
        //collection.updateMany(new BasicDBObject(),document);
    }

    public void deleteOneTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //申明删除条件
        Bson filter = Filters.eq("id",3);
        //删除与筛选器匹配的单个文档
        collection.deleteOne(filter);

        //删除与筛选器匹配的所有文档
        // collection.deleteMany(filter);

        System.out.println("--------删除所有文档----------");
        //删除所有文档
        // collection.deleteMany(new Document());
    }

}
