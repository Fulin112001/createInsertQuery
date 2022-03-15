package sqlToTxt;

//import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.security.spec.AlgorithmParameterSpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Hex;
import org.json.simple.parser.JSONParser;

import es.gob.afirma.core.misc.BoundedBufferedReader;

public class sqlToTxt {

	public static void main(String[] args) throws IOException, SQLException {

		String dbUrl = "";
		// Get path of the JAR file
		String jarPath = "";
		File jarDir = new File(ClassLoader.getSystemClassLoader().getResource(".").getPath());
		jarPath = jarDir.getAbsolutePath();
		jarPath = jarPath.concat(File.separator) + "";

		// Check and read config file
		String configFilePath = jarPath.concat("conf").concat(File.separator).concat("app.conf");
		File fConf = new File(configFilePath);
		if (!fConf.exists() || fConf.isDirectory()) {
			System.err.println(fConf.getPath());
			System.err.println("config file not found.");
			return;
		}

		JSONParser parser = new JSONParser();
		// Read config file
		FileReader fr = new FileReader(configFilePath);
		BoundedBufferedReader br = new BoundedBufferedReader(fr);
		String oneLine, dbIp = "", dbPort = "", dbName = "", dbUser = "", dbPass = "", dbNonce = "";

		while ((oneLine = br.readLine()) != null) {
			oneLine = oneLine.trim();
			if (!oneLine.isEmpty()) {
				if (!oneLine.startsWith("#")) {
					String[] parts = oneLine.split("=");
					if (parts.length == 2) {
						String key = parts[0].trim();
						String value = parts[1].trim();

						if ("db_host".equalsIgnoreCase(key))
							dbIp = value;
						else if ("db_port".equalsIgnoreCase(key))
							dbPort = value;
						else if ("db_name".equalsIgnoreCase(key))
							dbName = value;
						else if ("db_user".equalsIgnoreCase(key))
							dbUser = value;
						else if ("db_pass".equalsIgnoreCase(key))
							dbPass = value;
						else if ("db_nonce".equalsIgnoreCase(key))
							dbNonce = value;
					}
				}
			}
		}
		if (!dbIp.isEmpty() && !dbName.isEmpty() && !dbUser.isEmpty() && !dbPass.isEmpty() && !dbNonce.isEmpty()) {
			String decrypted;
			try {
				decrypted = decrypt(dbPass, dbNonce).trim();
				if (!decrypted.isEmpty()) {
					dbUrl = "jdbc:sqlserver://".concat(dbIp);
					if (!dbPort.isEmpty())
						dbUrl = dbUrl.concat(":").concat(dbPort);
					dbUrl = dbUrl.concat(";databaseName=").concat(dbName)
							.concat(";useBulkCopyForBatchInsert=true;user=").concat(dbUser).concat(";password=")
							.concat(decrypted);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		br.close();

		// PARAMETERS
		if (args.length > 0) {
			final String noApply = args[0].toString();// MD5加密eep做 不需要在這做
			final String tables = args[1].toString(); // 主表+detail
			final String template = args[2].toString();
			final String OUTPUT_FILE_PATH = args[3].toString();
			final String OUTPUT_FILE_NAME = "Exp_Apply_" + noApply + ".sql";
			final String OUTPUT_FILE = OUTPUT_FILE_PATH + OUTPUT_FILE_NAME;

			String output = sqlToTxt(tables, noApply, template, OUTPUT_FILE, dbUrl);
			String[] tableStr = tables.split(";");
			String mainTbl = tableStr[0];
			String detailTbl = tableStr[1];
			int sqlRlt = sqlExec(mainTbl, noApply, OUTPUT_FILE_NAME, dbUrl);
			if (output.length() > 0 && sqlRlt > 0) {
				System.out.println(true);
			} else {
				System.out.println(false);
			}
		} else {
			// PARAMETERS test
			final String noApply = "R11103001";// MD5加密eep做 不需要在這做
			final String tables = "RPT1_Apply;RPT1_Apply_ACD01A"; // 主表+detail
			final String template = "/Users/fulintseng/Desktop/workspace/SqlToTxt/build/classes/conf/RPT1_Apply_ACD01.txt";
			final String OUTPUT_FILE_PATH = "/Users/fulintseng/Desktop/workspace/SqlToTxt/";
			final String OUTPUT_FILE_NAME = "Exp_Apply_" + noApply + ".sql";
			final String OUTPUT_FILE = OUTPUT_FILE_PATH + OUTPUT_FILE_NAME;

			String[] tableStr = tables.split(";");
			String mainTbl = tableStr[0];
			String output = sqlToTxt(tables, noApply, template, OUTPUT_FILE, dbUrl);
			int sqlRlt = sqlExec(mainTbl, noApply, OUTPUT_FILE_NAME, dbUrl);
			if (output.length() > 0 && sqlRlt > 0) {
				System.out.println(true);
			} else {
				System.out.println(false);
			}

		}
	}

	private static int sqlExec(String TABLE_NAME, String noApply, String OUTPUT_FILE_NAME, String connect)
			throws SQLException {

		Connection conn;
		conn = getConn(connect);
		String execScript = "UPDATE [" + TABLE_NAME + "] SET [Ds_Import_File]='" + OUTPUT_FILE_NAME
				+ "', [Dt_Import]=getDate() where [No_Apply] = '" + noApply + "';";
		PreparedStatement preparedStatement = conn.prepareStatement(execScript);
		int updateValue = preparedStatement.executeUpdate();
		return updateValue;
	}

	/**
	 * 
	 * Connect to database
	 * 
	 * @param s   原始字符串
	 * @param num 指定的字節長度
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	public static String sqlToTxt(String tables, String noApply, String template, String OUTPUT_FILE, String connect)
			throws IOException {

		Connection conn;
		String out = "";
		try {
			conn = getConn(connect);
			try {

				String[] tableStr = tables.split(";");
				String mainTbl = tableStr[0];
				String detailTbl = tableStr[1];

				String mainData = createSql(mainTbl, noApply, conn);
				if (!mainTbl.isEmpty() && mainData.trim().length() > 0) {
					out = out + "--主表：" + mainData;
				}
				String detail = createSql(detailTbl, noApply, conn);
				if (!detailTbl.isEmpty() && detail.trim().length() > 0) {
					out = "\n--主表：" + mainData + "\n\n--明細表：" + detail;
				}
				String declareSql = "\n\n--參數\nDECLARE @No_Apply   nvarchar(50)\n  SET @No_Apply ='" + noApply + "';";
				out = out + declareSql;

				StringBuffer buffer = new StringBuffer();
				// Reader
				FileInputStream fis = new FileInputStream(new File(template));
				InputStreamReader reader = new InputStreamReader(fis, "UTF8");// StandardCharsets.ISO_8859_1

				int ch;
				while ((ch = reader.read()) != -1) {
					buffer.append((char) ch);
				}

//		    --------------RAW MATERIAL----------------------
				String templateData = buffer.toString();
				out = out + templateData;

				// File output
				try {
					Writer Newout;
					Newout = new OutputStreamWriter(new FileOutputStream(new File(OUTPUT_FILE)), "UTF-8");
					Newout.write(out);
					Newout.close();
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return out;

	}

	private static Connection getConn(String connect) throws SQLException {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		Connection connection = null;
		connection = DriverManager.getConnection(connect);
		return connection;
	}

	private static String createSql(String TABLE_NAME, String noApply, Connection connection) throws SQLException {

		String selectSql = "SELECT * FROM " + TABLE_NAME;
		String whereSql = " WHERE NO_APPLY='" + noApply + "'";
//		System.out.println(selectSql + whereSql);

		String prepareColumns = " (";
		Statement statement = connection.createStatement();
		ResultSet resultSet = null;
		resultSet = statement.executeQuery(selectSql + whereSql);

		ResultSetMetaData rsmd = resultSet.getMetaData();
		int colCnt = rsmd.getColumnCount();// include ID
		for (int i = 1; i <= colCnt; i++) {
			if (i > 1) {
				// insert without ID
				prepareColumns += rsmd.getColumnName(i) + ", ";
			}
		}
		prepareColumns = prepareColumns.substring(0, prepareColumns.length() - 2) + " )VALUES(";
		String out = "";
		// Print results from select statement

		while (resultSet.next()) {
			String query = "\nINSERT INTO " + TABLE_NAME + prepareColumns;
			for (int j = 1; j <= colCnt; j++) {
				if (j > 1) {
					String val = resultSet.getString(j);
					if (null != val) {
						query = query + "'" + val + "',";
					} else {
						query = query + "'',";
					}
				}
				if (j == colCnt) {
					query = query.substring(0, query.length() - 1) + ");";
				}
			}
			out = query;
		}
		return out;
	}

	/**
	 * Decrypts encrypted password. param cipherText : ciphertext param nonceText :
	 * nonce data to verify on decryption with GCM auth tag return original
	 * plaintext
	 */
	private static String decrypt(String cipherText, String nonceText) throws Exception {
		byte[] secretKeyBytes = "l0y@le8k9v46ef83eh9if0k6ny6oxi7n".getBytes();
		byte[] nonce = Hex.decodeHex(nonceText);
		byte[] cipherMessage = Hex.decodeHex(cipherText);
		String result = "";
		try {
			SecretKey secretKey = new SecretKeySpec(secretKeyBytes, "AES");
			AlgorithmParameterSpec spec = new GCMParameterSpec(128, nonce); // , 0, GCM_IV_LENGTH);
			Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
			cipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
			byte[] plainText = cipher.doFinal(cipherMessage);
			result = new String(plainText);
		} catch (IllegalStateException e) {
		} catch (javax.crypto.IllegalBlockSizeException e) {
		} catch (javax.crypto.BadPaddingException e) {
		}
		return result;
	}

}
