import java.io.*;
import java.sql.*;

public class Main {
    public static void main(String[] args) {
        Main m = new Main();
        m.createTableShops();
        m.createTableOrders();
        m.readTableShops("market_1.csv");
        m.readTableOrders("billing_2.csv");
        m.selectAllFromOrdersInnerJoinShops();
    }

    public void readTableShops(String filename) {
        String line = "";
        String[] table = new String[2];
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                table = line.split(",");
                insertIntoShops(Integer.parseInt(table[0]), table[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readTableOrders(String filename) {
        String line = "";
        String[] table = new String[3];
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                table = line.split(",");
                insertIntoOrders(Integer.parseInt(table[0]), Integer.parseInt(table[1]), Integer.parseInt(table[2]));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Connection connect() {
        //String url = "jdbc:sqlite::memory:";
        String url = "jdbc:sqlite:test.db";
        Connection conn = null;
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection(url);
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public void createTableShops() {
        String sql = "CREATE TABLE IF NOT EXISTS shops (\n"
                + "	shop_id integer UNIQUE PRIMARY KEY,\n"
                + "	shop_name text NOT NULL\n"
                + ");";
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createTableOrders() {
        String sql = "CREATE TABLE IF NOT EXISTS orders (\n"
                + "	order_id integer UNIQUE PRIMARY KEY,\n"
                + "	shop_id integer NOT NULL,\n"
                + "	cost integer\n"
                + ");";
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertIntoShops(int shopId, String shopName) {
        String sql = "INSERT INTO shops(shop_id,shop_name) VALUES(?,?)";
        try (Connection conn = this.connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, shopId);
            pstmt.setString(2, shopName);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertIntoOrders(int orderId, int shopId, int cost) {
        String sql = "INSERT INTO orders(order_id,shop_id,cost) VALUES(?,?,?)";
        try (Connection conn = this.connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, orderId);
            pstmt.setInt(2, shopId);
            pstmt.setInt(3, cost);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void selectAllFromOrdersInnerJoinShops() {
        String sql = "SELECT orders.order_id AS order_id, shops.shop_name AS shop_name, orders.shop_id AS shop_id," +
                " orders.cost AS cost FROM orders INNER JOIN shops ON orders.shop_id = shops.shop_id";

        try (Connection conn = this.connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.println(rs.getInt("order_id") + "," +
                        rs.getString("shop_name") + "," +
                        rs.getInt("shop_id") + "," +
                        rs.getInt("cost"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
