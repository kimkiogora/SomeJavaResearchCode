/**
 * Author: Kim Kiogora <kimkiogora@gmail.com> Usage : Read Large Files + Update
 * database.
 */
package readlargefile;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 *
 * @author kkiogora
 */
public class ReadLargeFile {

    /**
     * Task to update the database.
     *
     * @param conn
     * @param queries
     */
    private void createTask(final Connection conn,
            final ArrayList<String> queries) {

        long start = System.currentTimeMillis();
        Statement stmt;
        try {
            // Set auto-commit to false
            conn.setAutoCommit(false);
            //stmt = conn.createStatement();
            stmt = conn.createStatement();
            for (String s : queries) {
                stmt.addBatch(s);
            }
            int rows_affected[] = stmt.executeBatch();
            stmt.close();

            conn.commit();

            System.out.println("Rows affected : "
                    + rows_affected.length);
        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }

        long stopb = System.currentTimeMillis();
        float elapsed = (stopb - start) / 1024;
        String strEl = String.format("%.2f", elapsed);
        System.out.println("TimeTaken in createTaskB sec ".concat(strEl));

    }

    /**
     * Task to delete records from the same file.
     *
     * @param file
     * @param retriedData
     */
    private void deleteQuery(final String file, final String retriedData) {

        List<String> queries;
        try {
            queries = loadFile(file, false);
            // Find a match to the query
            if (queries.contains(retriedData)) {
                queries.remove(retriedData);
            }

            // Now save the file
            PrintWriter pout = new PrintWriter(new FileOutputStream(file,
                    false));

            for (String newQueries : queries) {
                pout.println(newQueries);
            }

            pout.flush();
            pout.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Reusable function to load file data.
     *
     * @param path
     * @return
     */
    private ArrayList<String> loadFile(String path, boolean debug) throws IOException {
        long start = System.currentTimeMillis();
        ArrayList<String> filedata = new ArrayList<String>();
        FileInputStream inputStream = null;
        Scanner sc = null;
        try {
            inputStream = new FileInputStream(path);
            sc = new Scanner(inputStream, "UTF-8");
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                if (!line.endsWith(";")) {
                    line = line.concat(";");
                }
                filedata.add(line);
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }

        if (debug) {
            long stopb = System.currentTimeMillis();
            float elapsed = (stopb - start) / 1024;
            String strEl = String.format("%.4f", elapsed);
            System.out.println("TimeTaken in loadFile() "
                    + "func in sec ".concat(strEl));
        }

        return filedata;
    }

    /**
     * Reprocess Large file sequentially.
     *
     * @param path
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void reprocessFailedQueuries(String path)
            throws FileNotFoundException, IOException {

        MySQL mysql = null;
        Connection conn = null;
        ArrayList<String> listx;

        try {
            mysql = new MySQL("localhost", "3306", "mydatabase", "******",
                    "******", "mypool", 3);
        } catch (ClassNotFoundException ce) {
            ce.printStackTrace();
        } catch (InstantiationException ie) {
            ie.printStackTrace();
        } catch (IllegalAccessException ile) {
            ile.printStackTrace();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        if (mysql == null) {
            return;
        }

        try {
            conn = mysql.getConnection();
        } catch (SQLException sqe) {
            sqe.printStackTrace();
        }

        if (conn == null) {
            return;
        }

        listx = loadFile(path,true);

        createTask(conn, listx);

        if(conn!=null){
            try{
                conn.close();
            }catch(SQLException ex){
                ex.printStackTrace();
            }
        }
        
        long rstart = System.currentTimeMillis();
        for (String l : listx) {
            deleteQuery(path, l);
        }

        long rstopb = System.currentTimeMillis();
        float relapsed = (rstopb - rstart) / 1024;
        String rstrEl = String.format("%.2f", relapsed);
        System.out.println("TimeTaken in deleting"
                + " FailedQueries file in  sec ".concat(rstrEl));
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String path = "LargeDBUPdatesFile.txt";
        ReadLargeFile lgf = new ReadLargeFile();
        long start = System.currentTimeMillis();
        try {
            lgf.reprocessFailedQueuries(path);
            long stopb = System.currentTimeMillis();
            float elapsed = (stopb - start) / 1024;
            String strEl = String.format("%.2f", elapsed);
            System.out.println("Total TimeTaken to reprocessFailedQueuries"
                    + " in sec ".concat(strEl));
        } catch (IOException io) {
            io.printStackTrace();
        }
    }
}
