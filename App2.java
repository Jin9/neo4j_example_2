import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class App2 {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		
		Class.forName("org.neo4j.jdbc.Driver");
		Connection connect = DriverManager.getConnection("jdbc:neo4j://localhost:7474","neo4j","---------"); // censor password
		
		// Load products.csv from 'http' link
		String sql = "LOAD CSV WITH HEADERS FROM 'http://data.neo4j.com/northwind/products.csv'"+
					 "AS row CREATE (n:Product)"+
					 "SET n = row,"+
					 "n.unitPrice = toFloat(row.unitPrice),"+
					 "n.unitsInStock = toInt(row.unitsInStock), n.unitsOnOrder = toInt(row.unitsOnOrder),"+
					 "n.reorderLevel = toInt(row.reorderLevel), n.discontinued = (row.discontinued <> '0')";
		
		int output;
		
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			output = ps.executeUpdate();
			System.out.println(output);
		};
		
		// Load categories.csv from 'http' link
		sql = "LOAD CSV WITH HEADERS FROM 'http://data.neo4j.com/northwind/categories.csv'"+
			  "AS row CREATE (n:Category)"+
			  "SET n = row";
		
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			output = ps.executeUpdate();
			System.out.println(output);
		};
		
		// Load suppliers.csv from 'http' link
		sql = "LOAD CSV WITH HEADERS FROM 'http://data.neo4j.com/northwind/suppliers.csv'"+
			  " AS row CREATE (n:Supplier)"+
			  "SET n = row";
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			output = ps.executeUpdate();
			System.out.println(output);
		};
		
		// create index on productID
		sql = "CREATE INDEX ON :Product(productID)";
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			output = ps.executeUpdate();
			System.out.println(output);
		};
		
		// create index on categoryID
		sql = "CREATE INDEX ON :Category(categoryID)";
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			output = ps.executeUpdate();
			System.out.println(output);
		};
		
		// create index on supplierID
		sql = "CREATE INDEX ON :Supplier(supplierID)";
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			output = ps.executeUpdate();
			System.out.println(output);
		};
		
		// create 'PART_OF' relationship
		sql = "MATCH (p:Product),(c:Category) "+
			         "WHERE p.categoryID = c.categoryID "+
			         "CREATE (p)-[:PART_OF]->(c)";
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			output = ps.executeUpdate();
			System.out.println(output);
		};
		
		// create 'SUPPLIES' relationship
		sql = "MATCH (p:Product),(s:Supplier) "+
			  "WHERE p.supplierID = s.supplierID "+
			  "CREATE (s)-[:SUPPLIES]->(p)";
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			output = ps.executeUpdate();
			System.out.println(output);
		};
		
	}

}
