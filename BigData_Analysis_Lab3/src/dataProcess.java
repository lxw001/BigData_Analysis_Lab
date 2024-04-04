import java.io.*;
import java.util.*;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import static org.neo4j.driver.Values.parameters;

public class dataProcess {
    //定义输入路径
    public static String inputEdgesPath = "./Data/0.edges";
    public static String inputCirclesPath = "./Data/0.circles";
    public static String inputFeatPath = "./Data/0.feat";
    public static String inputFeatNamesPath = "./Data/0.featnames";

    public static String outputEdgesPath = "./src/edges.csv";
    public static String outputCirclePath = "./src/circle.csv";
    public static String outputFeaturePath = "./src/feature.csv";

    public static void handleEdges() throws IOException {
        BufferedReader br = null;
        String line;
        BufferedWriter bw = null;
        try {
            File inputFile = new File(inputEdgesPath);
            br = new BufferedReader(new FileReader(inputFile));
            FileWriter fw = new FileWriter(outputEdgesPath);
            bw = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while ((line = br.readLine()) != null) {
            String str[];
            String tempStr = "";
            //System.out.println(line);
            str = line.trim().split(" ");
            tempStr += str[0] + "," + str[1];
            //System.out.println(tempStr);
            bw.write(tempStr);
            bw.newLine();
        }
        bw.close();
    }

    public static void handleFeature() throws IOException {
        String line;
        File inputFeat = new File(inputFeatPath);
        File inputFeatNames = new File(inputFeatNamesPath);
        BufferedReader br = new BufferedReader(new FileReader(inputFeatNames));
        BufferedReader br2 = new BufferedReader(new FileReader(inputFeat));
        FileWriter fw = new FileWriter(outputFeaturePath);
        BufferedWriter bw = new BufferedWriter(fw);

        ArrayList<String> attributeNameList = new ArrayList<>();
        ArrayList<String> attributeValueList = new ArrayList<>();
        HashMap<String,String> map=new HashMap<>();//存储单值映射，一个属性多值则只存储最后一个值

        while ((line = br.readLine()) != null) {
            String str[];
            str = line.trim().split(" ");//按空格分割
            attributeNameList.add(str[1].replace(";anonymized", ""));
            attributeValueList.add(str[3]);
            map.put(str[1].replace(";anonymized", ""),str[3]);
        }

        //添加表头
        String tableHeader = "nodeId";
        Set<String> set = new HashSet();//存储不重复的属性名，但是没有顺序
        ArrayList<String> list=new ArrayList<>();//存储按顺序排列的不重复属性名

        for (String s : attributeNameList) {
            if (!set.contains(s)) {
                tableHeader = tableHeader + "," + s;
                set.add(s);
                list.add(s);
            }
        }
        //System.out.println(tableHeader);
        bw.write(tableHeader);
        bw.newLine();

        while ((line = br2.readLine()) != null) {
            String str[];
            String tempStr="";
            str = line.trim().split(" ");//按多个或一个空格分割
            int nodeId=Integer.valueOf(str[0]);
            tempStr=tempStr+nodeId;
            Set<String> tempSet=new HashSet<>();//存储Person属性名
            //判断每个person有哪些属性
            for (int i = 1; i < str.length; i++) {
                if(str[i].equals("1")) tempSet.add(attributeNameList.get(i-1));
            }

            for(String s:list){
                //按顺序遍历属性名列表，如果无该属性，则填入-1，否则填入map中纯初的值
                if(tempSet.contains(s)) tempStr=tempStr+","+map.get(s);
                else tempStr=tempStr+","+ (-1);
            }
            bw.write(tempStr);
            bw.newLine();
        }

        bw.close();
    }


    public static void handleCircles() throws IOException {
        String line;
        File inputFile = new File(inputCirclesPath);
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        FileWriter fw = new FileWriter(outputCirclePath);
        BufferedWriter bw = new BufferedWriter(fw);


        while ((line = br.readLine()) != null) {
            String str[];
            String tempStr = "";
            //System.out.println(line);
            str = line.trim().split("\\s+");//按多个或一个空格分割
            for (String s : str)
                tempStr = tempStr + "," + s;
            tempStr = tempStr.substring(1);
            //System.out.println(tempStr);
            bw.write(tempStr);
            bw.newLine();
        }
        bw.close();
    }

    public  static  void importNodes() throws IOException {
        String line;
        String query="";
        List<Map<String, Object>> parametersList = new ArrayList<>();

        File inputFile = new File(outputFeaturePath);
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        line= br.readLine();
        while ((line = br.readLine()) != null) {
            String str[];
            String tempStr = "";
            //System.out.println(line);
            str = line.trim().split(",");//按多个或一个空格分割
            for (String s : str)
                tempStr = tempStr + "," + s;
            tempStr = tempStr.substring(1);
            //System.out.println(tempStr);
        }
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", 	AuthTokens.basic("neo4j", "Sb666666"));
        Session session = driver.session();
        session.run( "CREATE (a:Person {name: $name, title: $title})",
                parameters( "name", "Arthur001", "title", "King001" ) );
        //循环创建结点
        //String query = "CREATE (n:Person {name: $name, age: $age})";

//        parametersList.add(ImmutableMap.of("name", "John Doe", "age", 25));
//        parametersList.add(ImmutableMap.of("name", "Jane Doe", "age", 30));
        session.writeTransaction(tx -> {
            for (Map<String, Object> parameters : parametersList) {
                tx.run(query, parameters);
            }
            return null;
        });

        session.close();
        driver.close();

    }

    public  static  void importRelationship() throws IOException{

    }

    public  static  void importCircles() throws IOException{

    }
    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        handleEdges();
//        handleFeature();
//        handleCircles();
          importNodes();
          importRelationship();
          importCircles();

    }
}
