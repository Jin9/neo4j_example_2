import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class App1 {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		
		Class.forName("org.neo4j.jdbc.Driver");
		Connection connect = DriverManager.getConnection("jdbc:neo4j://localhost:7474","neo4j","---------"); // censor password
		
		// Create data below by neo4jjdbc
		String sql = "CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})"+
					 "CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})"+
					 "CREATE (Carrie:Person {name:'Carrie-Anne Moss', born:1967})"+
					 "CREATE (Laurence:Person {name:'Laurence Fishburne', born:1961})"+
					 "CREATE (Hugo:Person {name:'Hugo Weaving', born:1960})"+
					 "CREATE (AndyW:Person {name:'Andy Wachowski', born:1967})"+
					 "CREATE (LanaW:Person {name:'Lana Wachowski', born:1965})"+
					 "CREATE (JoelS:Person {name:'Joel Silver', born:1952})"+
					 "CREATE (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix),"+
					 		"(Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrix),"+
					 		"(Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrix),"+
					 		"(Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrix),"+
					 		"(AndyW)-[:DIRECTED]->(TheMatrix),"+
					 		"(LanaW)-[:DIRECTED]->(TheMatrix),"+
					 		"(JoelS)-[:PRODUCED]->(TheMatrix)";
		
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			int output = ps.executeUpdate();
			System.out.println(output);
		};
		
		// remove 'Lana Wachowski' from Directed
		sql = "MATCH (:Person {name:'Lana Wachowski'})-[r:DIRECTED]->() DELETE r";
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			int output = ps.executeUpdate();
			System.out.println(output);
		};
		
		
		// Update Person name
		sql = "CREATE (Jin:Person {name:'Jin Wongwises', born:1992})";
		try(PreparedStatement ps = connect.prepareStatement(sql)){
			int output = ps.executeUpdate();
			System.out.println(output);
		};
		
		
		// Query All Actor in Matrix
		sql = "MATCH (p:Person)-[r:ACTED_IN]->() RETURN p,r";
		Statement stmt = connect.createStatement();
		ResultSet res = stmt.executeQuery(sql);
		int count = 1;
		while(res.next()){
			ResultSetMetaData resm = res.getMetaData();
			System.out.print ( count++ );
			for(int i=1, cols = resm.getColumnCount(); i <= cols ; i++ ){
				String r = resm.getColumnLabel(i) + " : " + res.getObject(i);
				System.out.println(" : "+r );
			}
			
		}
		
		
		
	}

}
