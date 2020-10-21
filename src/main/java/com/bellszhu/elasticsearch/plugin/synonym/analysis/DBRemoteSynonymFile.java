package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import com.bellszhu.elasticsearch.plugin.DynamicSynonymPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.env.Environment;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author husc
 * @date 2020/9/25
 * 从数据库拉取同义词
 */
public class DBRemoteSynonymFile implements SynonymFile {
    // 配置文件名
    private final static String DB_PROPERTIES = "jdbc-reload.properties";
    private static Logger logger = LogManager.getLogger("DBRemoteSynonymFile");

    private String format;
    private boolean expand;
    private boolean lenient;
    private Analyzer analyzer;
    private Environment env;
    private String location; // 配置常量 "fromDb"
    private long lastModified;
    private Connection connection = null;
    private Statement statement = null;
    private Properties props;
    private Path conf_dir;

    public DBRemoteSynonymFile(Environment env, Analyzer analyzer, boolean expand, boolean lenient, String format, String location) {
        this.analyzer = analyzer;
        this.expand = expand;
        this.format = format;
        this.lenient = lenient;
        this.env = env;
        this.location = location;
        this.props = new Properties();
        // 读取当前 jar 包存放的路径
        Path filePath = PathUtils.get(new File(DynamicSynonymPlugin.class.getProtectionDomain().getCodeSource()
                .getLocation().getPath())
                .getParent(), "config")
                .toAbsolutePath();
        this.conf_dir = filePath.resolve(DB_PROPERTIES);

        // 判断文件是否存在
        File configFile = conf_dir.toFile();
        InputStream input = null;
        try {
            input = new FileInputStream(configFile);
        } catch (FileNotFoundException e) {
            logger.error("jdbc-reload.properties not find.", e);
        }
        if (input != null) {
            try {
                props.load(input);
            } catch (IOException e) {
                logger.error("fail to load the jdbc-reload.properties", e);
            }
        }
        isNeedReloadSynonymMap();
    }

    /**
     * 加载同义词词典至SynonymMap中
     *
     * @return SynonymMap
     */
    @Override
    public SynonymMap reloadSynonymMap() {
        try {
            logger.info("start reload local synonym from location=[{}].", location);
            Reader rulesReader = getReader();
            SynonymMap.Builder parser = RemoteSynonymFile.getSynonymParser(rulesReader, format, expand, lenient, analyzer);
            return parser.build();
        } catch (Exception e) {
            logger.error(String.format("reload local synonym from location=[%s] error!", location), e);
            throw new IllegalArgumentException("could not reload local synonyms file to build synonyms", e);
        }
    }

    /**
     * 判断是否需要进行重新加载
     *
     * @return true or false
     */
    @Override
    public boolean isNeedReloadSynonymMap() {
        try {
            Long lastModify = getLastModify();
            if (lastModified < lastModify) {
                lastModified = lastModify;
                return true;
            }
        } catch (Exception e) {
            logger.error("judge if a reload is required exception!", e);
        }
        return false;
    }


    /**
     * 同义词库的加载
     *
     * @return Reader
     */
    @Override
    public Reader getReader() {
        StringBuilder sb = new StringBuilder();
        try {
            ArrayList<String> dbData = getDBData();
            logger.info(">>>>>> start loading the synonym from db...");
            for (int i = 0; i < dbData.size(); i++) {
                logger.info(dbData.get(i));
                sb.append(dbData.get(i)).append(System.getProperty("line.separator"));
            }
            logger.info(">>>>>> finish loading the synonym from db.");
        } catch (Exception e) {
            logger.error("reload synonym from db failed");
        }
        return new StringReader(sb.toString());
    }

    /**
     * 查询数据库中的同义词
     *
     * @return DBData
     */
    private ArrayList<String> getDBData() {
        ArrayList<String> arrayList = new ArrayList<>();
        ResultSet resultSet = null;
        try {
            if (connection == null || statement == null) {
                String driver = props.getProperty("jdbc.driver");
                String url = props.getProperty("jdbc.url");
                String user = props.getProperty("jdbc.user");
                String password = props.getProperty("jdbc.password");
                Class.forName(driver);
                connection = DriverManager.getConnection(url, user, password);
                statement = connection.createStatement();
            }
            long t1 = System.currentTimeMillis();
            resultSet = statement.executeQuery(props.getProperty("jdbc.reload.synonym.sql"));
            long t2 = System.currentTimeMillis();
            logger.info("初始化全量jdbc.reload.synonym.sql耗时" + (t2 - t1) + "ms!");
            while (resultSet.next()) {
                String theWord = resultSet.getString("words");
                arrayList.add(theWord);
            }
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("get data from db failed!", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return arrayList;
    }

    /**
     * 获取同义词库最后一次修改的时间
     * 用于判断同义词是否需要进行重新加载
     *
     * @return getLastModify
     */
    private Long getLastModify() {
        ResultSet resultSet = null;
        Long last_modify_long = null;
        try {
            if (connection == null || statement == null) {
                String driver = props.getProperty("jdbc.driver");
                String url = props.getProperty("jdbc.url");
                String user = props.getProperty("jdbc.user");
                String password = props.getProperty("jdbc.password");
                Class.forName(driver);
                connection = DriverManager.getConnection(url, user, password);
                statement = connection.createStatement();
            }
            long t1 = System.currentTimeMillis();
            resultSet = statement.executeQuery(props.getProperty("jdbc.lastModified.synonym.sql"));
            long t2 = System.currentTimeMillis();
            logger.info("定时任务增量jdbc.lastModified.synonym.sql耗时" + (t2 - t1) + "ms!");
            while (resultSet.next()) {
                Timestamp last_modify_dt = resultSet.getTimestamp("last_modify_dt");
                last_modify_long = last_modify_dt != null ? last_modify_dt.getTime() : -1;
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return last_modify_long;
    }
}
