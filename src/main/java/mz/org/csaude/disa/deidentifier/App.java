package mz.org.csaude.disa.deidentifier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.github.javafaker.Faker;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

/**
 * Hello world!
 *
 */
public class App {

    private static final int MYSQL_ER_DUP_KEYNAME = 1061;

    private static final Logger log = Logger.getLogger(App.class.getName());

    private static final String DEFAULT_USER = System.getProperty("user.name");
    private static final String DB_NAME_ARG = "db_name";
    private static final int DEFAULT_PORT = 3306;
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_HOST = "localhost";

    public static void main(String[] args) throws ClassNotFoundException {

        Class.forName("com.mysql.cj.jdbc.Driver");

        Options options = new Options();
        options.addOption("h", "host", true, "Connect to the MySQL server on the given host.");
        options.addOption("P", "port", true,
                "For TCP/IP connections, the port number to use.");
        options.addOption("u", "user", true,
                "The user name of the MySQL account to use for connecting to the server.");
        options.addOption("p", "password", true,
                "The password of the MySQL account used for connecting to the server.");
        options.addOption("v", "verbose", false,
                "Produce more output about what the program does.");

        CommandLine cmd = null;
        try {
            CommandLineParser parser = new DefaultParser();
            cmd = parser.parse(options, args);

            if (cmd.getArgList().isEmpty()) {
                throw new ParseException("Missing required arg: " + DB_NAME_ARG);
            }

            String host = cmd.hasOption("h") ? cmd.getOptionValue("h") : DEFAULT_HOST;
            int port = cmd.hasOption("P") ? Integer.valueOf(cmd.getOptionValue("P")) : DEFAULT_PORT;
            String database = cmd.getArgs()[0];
            String username = cmd.hasOption("u") ? cmd.getOptionValue("u") : DEFAULT_USER;
            String password = cmd.hasOption("p") ? cmd.getOptionValue("p") : DEFAULT_PASSWORD;
            String connString = String.format("jdbc:mysql://%s:%d/%s", host, port, database);

            try (Connection connection = DriverManager.getConnection(connString, username, password)) {
                new App(connection, new Faker()).run();
            }

        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            log.info(e.getMessage());
            formatter.printHelp("de-identify " + DB_NAME_ARG, options);
        } catch (SQLException e) {
            log.log(Level.SEVERE, e.getMessage());
            if (cmd != null && cmd.hasOption("v")) {
                e.printStackTrace();
            }
        }
    }

    private Connection connection;
    private Faker faker;
    private ProgressBarBuilder pbBuilder;

    public App(Connection connection, Faker faker) {
        this.connection = connection;
        this.faker = faker;
        this.pbBuilder = new ProgressBarBuilder()
                .setTaskName("De-identifying")
                .setStyle(ProgressBarStyle.ASCII);
    }

    private void run() throws SQLException {
        creatIndexOnUniqueId();
        deIdentifyPatients(getNids());
        log.log(Level.FINE, "Done.");
    }

    private void creatIndexOnUniqueId() throws SQLException {
        String ddl = "CREATE INDEX `idx_VlData_UNIQUEID`  ON `VlData` (UNIQUEID)";
        try (Statement statement = connection.createStatement()) {
            log.log(Level.FINE, "Creating index on UNIQUEID");
            statement.execute(ddl);
        } catch (SQLException e) {
            if (e.getErrorCode() == MYSQL_ER_DUP_KEYNAME) {
                log.log(Level.FINE, "Index already created.");
            } else {
                throw e;
            }
        }
    }

    private List<String> getNids() throws SQLException {
        List<String> nids = new ArrayList<>();
        String sql = "SELECT UNIQUEID FROM VlData GROUP BY UNIQUEID";
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {

            while (resultSet.next()) {
                nids.add(resultSet.getString(1));

            }
        }
        return nids;
    }

    private void deIdentifyPatients(List<String> nids) throws SQLException {
        String sql = "UPDATE VlData SET FIRSTNAME=?, SURNAME=?, UNIQUEID=substring(sha1(trim(UNIQUEID)), 1, 31) WHERE UNIQUEID=?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (String n : ProgressBar.wrap(nids, pbBuilder)) {
                preparedStatement.setString(1, faker.name().firstName());
                preparedStatement.setString(2, faker.name().lastName());
                preparedStatement.setString(3, n);
                preparedStatement.executeUpdate();
            }
        }
    }
}
