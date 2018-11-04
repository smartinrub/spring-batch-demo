package org.smartinrub.springbatchdemo;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class BatchConfiguration {

    private static final String SQL_SELECT_QUERY = "SELECT * FROM credentials";
    private static final String SQL_UPDATE_QUERY = "UPDATE credentials SET password = :password WHERE id = :id ";
    public static final String SQL_INSERT_QUERY = "INSERT INTO credentials_backup VALUES(:id, :password)";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public JdbcCursorItemReader<Credentials> reader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<Credentials>()
                .name("credentialsReader")
                .dataSource(dataSource)
                .sql(SQL_SELECT_QUERY)
                .rowMapper(new CredentialsRowMapper())
                .build();
    }

    @Bean
    public PasswordProcessor processor() {
        return new PasswordProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Credentials> writer1(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Credentials>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql(SQL_UPDATE_QUERY)
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Credentials> writer2(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Credentials>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql(SQL_INSERT_QUERY)
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Job credentialsJob(JobCompletionNotificationListener listener, Step step1, Step step2) {
        return jobBuilderFactory.get("credentialsJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(step1)
                .next(step2)
                .preventRestart()
                .build();
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<Credentials> writer2, JdbcCursorItemReader<Credentials> reader) {
        return stepBuilderFactory.get("step1")
                .<Credentials, Credentials>chunk(4)
                .reader(reader)
                .writer(writer2)
                .build();
    }

    @Bean
    public Step step2(JdbcBatchItemWriter<Credentials> writer1, JdbcCursorItemReader<Credentials> reader) {
        return stepBuilderFactory.get("step2")
                .<Credentials, Credentials>chunk(4)
                .reader(reader)
                .processor(processor())
                .writer(writer1)
                .build();
    }

    public class CredentialsRowMapper implements RowMapper<Credentials> {

        @Override
        public Credentials mapRow(ResultSet resultSet, int i) throws SQLException {
            return new Credentials(
                    resultSet.getString("id"),
                    resultSet.getString("password"));
        }
    }
}
