package uk.ac.ed.inf.acpTutorial.service;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import uk.ac.ed.inf.acpTutorial.configuration.DynamoDbConfiguration;
import uk.ac.ed.inf.acpTutorial.configuration.SystemEnvironment;
import uk.ac.ed.inf.acpTutorial.dto.Drone;
import uk.ac.ed.inf.acpTutorial.entity.DroneEntity;
import uk.ac.ed.inf.acpTutorial.mapper.DroneMapper;
import uk.ac.ed.inf.acpTutorial.repository.DroneRepository;

import java.net.URI;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import java.util.Map;
import java.util.Optional;
@Slf4j
@Service
public class DynamoDbService {

    private final DynamoDbConfiguration dynamoDbConfiguration;
    private final SystemEnvironment systemEnvironment;
    private static final String KEY_COLUMN_NAME = "key";

    public DynamoDbService(DynamoDbConfiguration dynamoDbConfiguration, SystemEnvironment systemEnvironment) {
        this.dynamoDbConfiguration = dynamoDbConfiguration;
        this.systemEnvironment = systemEnvironment;
    }

    public List<String> listTables() {
        return getDynamoDbClient().listTables().tableNames();
    }


    public List<String> listTableObjects(@PathVariable String table) {
        return getDynamoDbClient()
                .scanPaginator(ScanRequest.builder()
                        .tableName(table)
                        .build())
                .items()
                .stream()
                .map(e ->
                    "{ \"key\": \"" + e.get("key").s() + " \", \"content\": \"" + e.get("content").s() + "\" } "
                )
                .toList();
    }

    public void createTable(@PathVariable String table) {
        getDynamoDbClient().createTable(b -> b.tableName(table)
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(KEY_COLUMN_NAME)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(KEY_COLUMN_NAME)
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
        );
    }

    public void createObject(@PathVariable String table, @PathVariable String key, @RequestBody String objectContent) {
        getDynamoDbClient().putItem(b -> b.tableName(table).item(
                java.util.Map.of("key", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(key).build(),
                        "content", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(objectContent).build())
        ));
    }

    public String getTablePrimaryKey(
            @Parameter(name = "table", description = "The name of the DynamoDB table")
            @PathVariable(required = true)
            String table) {

        DescribeTableRequest request = DescribeTableRequest.builder()
                .tableName(table)
                .build();

        DescribeTableResponse response = getDynamoDbClient().describeTable(request);

        return response.table().keySchema().stream()
                .filter(k -> k.keyType().toString().equals("HASH"))
                .map(KeySchemaElement::attributeName)
                .findFirst()
                .orElseThrow();
    }



    private DynamoDbClient getDynamoDbClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(dynamoDbConfiguration.getDynamoDbEndpoint()))
                .region(systemEnvironment.getAwsRegion())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(systemEnvironment.getAwsUser(), systemEnvironment.getAwsSecret())))
                .build();
    }

    public void saveMessageToDynamoDb(String sqsTableInDynamoDb, String key, String message) {
        if (! getDynamoDbClient().listTables().tableNames().contains(sqsTableInDynamoDb)) {
            createTable(sqsTableInDynamoDb);
        }
        createObject(sqsTableInDynamoDb, key, message);
    }

    public List<String> getAllContents(String table) {
        return getDynamoDbClient()
                .scanPaginator(ScanRequest.builder().tableName(table).build())
                .items()
                .stream()
                .map(item -> item.getOrDefault("content", AttributeValue.builder().s("{}").build()).s())
                .toList();
    }

    public Optional<String> getSingleItem(String table, String key) {
        var response = getDynamoDbClient().getItem(GetItemRequest.builder()
                .tableName(table)
                .key(Map.of("key", AttributeValue.builder().s(key).build()))
                .build());
        if (!response.hasItem() || response.item().isEmpty()) return Optional.empty();
        return Optional.ofNullable(response.item().get("content").s());
    }
}
