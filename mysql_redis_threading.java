package com.example.efficientdataprocessing;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

@SpringBootApplication
public class EfficientDataProcessingApplication implements CommandLineRunner {

    private static final Logger LOGGER = Logger.getLogger(EfficientDataProcessingApplication.class.getName());

    @Autowired
    private DataProcessingService dataProcessingService;

    public static void main(String[] args) {
        SpringApplication.run(EfficientDataProcessingApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        dataProcessingService.processData();
    }
}

@Service
class DataProcessingService {

    private static final Logger LOGGER = Logger.getLogger(DataProcessingService.class.getName());

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private Executor taskExecutor;

    public void processData() {
        CompletableFuture.runAsync(this::fetchAndCacheData, taskExecutor)
                .thenRunAsync(this::updateDatabase, taskExecutor)
                .exceptionally(ex -> {
                    LOGGER.severe("Error during data processing: " + ex.getMessage());
                    return null;
                });
    }

    private void fetchAndCacheData() {
        try {
            String query = "SELECT * FROM your_table WHERE condition = ?";
            Object result = jdbcTemplate.queryForList(query, "value");
            redisTemplate.opsForValue().set("cacheKey", result);
            LOGGER.info("Data fetched and cached successfully.");
        } catch (Exception e) {
            LOGGER.severe("Error fetching data: " + e.getMessage());
        }
    }

    private void updateDatabase() {
        try {
            String updateQuery = "UPDATE your_table SET column = ? WHERE condition = ?";
            jdbcTemplate.update(updateQuery, "newValue", "value");
            LOGGER.info("Database updated successfully.");
        } catch (Exception e) {
            LOGGER.severe("Error updating database: " + e.getMessage());
        }
    }
}

@Configuration
class AppConfig {

    @Bean
    public DataSource dataSource() {
        
        return DataSourceBuilder.create()
                .url("jdbc:mysql://localhost:3306/your_database?useSSL=true")
                .username("your_username")
                .password("your_password")
                .build();
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(new LettuceConnectionFactory());
        return redisTemplate;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(25);
        executor.setThreadNamePrefix("TaskExecutor-");
        executor.initialize();
        return executor;
    }
}
