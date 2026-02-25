package uk.ac.ed.inf.acpTutorial.controller;

import com.google.gson.Gson;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;
import uk.ac.ed.inf.acpTutorial.configuration.S3Configuration;
import uk.ac.ed.inf.acpTutorial.configuration.SystemEnvironment;
import uk.ac.ed.inf.acpTutorial.service.DynamoDbService;
import uk.ac.ed.inf.acpTutorial.service.PostgresService;

import java.net.URI;
import java.util.*;

@RestController
@RequestMapping("/api/v1/acp")
public class AssignmentController {

    record UrlPathRequest(String urlPath) {}

    private final S3Configuration s3Configuration;
    private final SystemEnvironment systemEnvironment;
    private final DynamoDbService dynamoDbService;
    private final PostgresService postgresService;
    private final RestTemplate restTemplate = new RestTemplate();
    private final Gson gson = new Gson();

    public AssignmentController(S3Configuration s3Configuration,
                                SystemEnvironment systemEnvironment,
                                DynamoDbService dynamoDbService,
                                PostgresService postgresService) {
        this.s3Configuration = s3Configuration;
        this.systemEnvironment = systemEnvironment;
        this.dynamoDbService = dynamoDbService;
        this.postgresService = postgresService;
    }

    // ── S3 reads ──────────────────────────────────────────────────────────────

    @GetMapping(value = "/all/s3/{bucket}", produces = "application/json")
    public ResponseEntity<String> getAllS3Objects(@PathVariable String bucket) {
        try {
            List<String> contents = new ArrayList<>();
            for (S3Object obj : getS3Client().listObjectsV2(b -> b.bucket(bucket)).contents()) {
                try {
                    contents.add(new String(getS3Client().getObject(b -> b.bucket(bucket).key(obj.key())).readAllBytes()));
                } catch (Exception e) {
                    // skip unreadable objects
                }
            }
            return ResponseEntity.ok("[" + String.join(",", contents) + "]");
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(value = "/single/s3/{bucket}/{key}", produces = "application/json")
    public ResponseEntity<String> getSingleS3Object(@PathVariable String bucket, @PathVariable String key) {
        try {
            String content = new String(getS3Client().getObject(b -> b.bucket(bucket).key(key)).readAllBytes());
            return ResponseEntity.ok(content);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    // ── DynamoDB reads ────────────────────────────────────────────────────────

    @GetMapping(value = "/all/dynamo/{table}", produces = "application/json")
    public ResponseEntity<String> getAllDynamoObjects(@PathVariable String table) {
        try {
            if (!dynamoDbService.listTables().contains(table)) {
                return ResponseEntity.notFound().build();
            }
            List<String> contents = dynamoDbService.getAllContents(table);
            return ResponseEntity.ok("[" + String.join(",", contents) + "]");
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(value = "/single/dynamo/{table}/{key}", produces = "application/json")
    public ResponseEntity<String> getSingleDynamoObject(@PathVariable String table, @PathVariable String key) {
        try {
            if (!dynamoDbService.listTables().contains(table)) {
                return ResponseEntity.notFound().build();
            }
            return dynamoDbService.getSingleItem(table, key)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    // ── Postgres reads ────────────────────────────────────────────────────────

    @GetMapping(value = "/all/postgres/{table}", produces = "application/json")
    public ResponseEntity<String> getAllPostgresRows(@PathVariable String table) {
        try {
            List<Map<String, Object>> rows = postgresService.getAllRowsFromTable(table);
            return ResponseEntity.ok(gson.toJson(rows));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    // ── process/dump ──────────────────────────────────────────────────────────

    @PostMapping("/process/dump")
    public ResponseEntity<List<Map<String, Object>>> processDump(@RequestBody UrlPathRequest request) {
        return ResponseEntity.ok(fetchAndEnrich(request.urlPath()));
    }

    // ── process/dynamo ────────────────────────────────────────────────────────

    @PostMapping("/process/dynamo")
    public ResponseEntity<Void> processDynamo(@RequestBody UrlPathRequest request) {
        String sid = s3Configuration.getS3Bucket();

        fetchAndEnrich(request.urlPath()).forEach(drone -> {
            String name = (String) drone.get("name");
            String json = gson.toJson(drone);
            dynamoDbService.createObject(sid, name, json);
        });

        return ResponseEntity.ok().build();
    }

    // ── process/s3 ────────────────────────────────────────────────────────────

    @PostMapping("/process/s3")
    public ResponseEntity<Void> processS3(@RequestBody UrlPathRequest request) {
        String sid = s3Configuration.getS3Bucket();

        fetchAndEnrich(request.urlPath()).forEach(drone -> {
            String name = (String) drone.get("name");
            String json = gson.toJson(drone);
            getS3Client().putObject(
                    b -> b.bucket(sid).key(name),
                    software.amazon.awssdk.core.sync.RequestBody.fromString(json)
            );
        });

        return ResponseEntity.ok().build();
    }

    // ── process/postgres ──────────────────────────────────────────────────────

    @PostMapping("/process/postgres/{table}")
    public ResponseEntity<Void> processPostgres(@PathVariable String table,
                                                @RequestBody UrlPathRequest request) {
        fetchAndEnrich(request.urlPath()).forEach(drone -> {
            Map<String, Object> cap = (Map<String, Object>) drone.get("capability");

            postgresService.insertDroneRow(table,
                    (String) drone.get("name"),
                    (String) drone.get("id"),
                    cap != null && Boolean.TRUE.equals(cap.get("cooling")),
                    cap != null && Boolean.TRUE.equals(cap.get("heating")),
                    cap != null ? ((Number) cap.get("capacity")).doubleValue()    : 0.0,
                    cap != null ? ((Number) cap.get("maxMoves")).intValue()       : 0,
                    cap != null ? ((Number) cap.get("costPerMove")).doubleValue() : 0.0,
                    cap != null ? ((Number) cap.get("costInitial")).doubleValue() : 0.0,
                    cap != null ? ((Number) cap.get("costFinal")).doubleValue()   : 0.0,
                    (double) drone.get("costPer100Moves")
            );
        });

        return ResponseEntity.ok().build();
    }

    // ── shared helpers ────────────────────────────────────────────────────────

    private List<Map<String, Object>> fetchAndEnrich(String url) {
        Map[] raw = restTemplate.getForObject(url, Map[].class);
        if (raw == null) return Collections.emptyList();

        List<Map<String, Object>> drones = new ArrayList<>(Arrays.asList((Map<String, Object>[]) raw));

        drones.forEach(drone -> {
            Map<String, Object> cap = (Map<String, Object>) drone.get("capability");
            double costPerMove = cap != null && cap.get("costPerMove") != null ? ((Number) cap.get("costPerMove")).doubleValue() : 0.0;
            double costInitial = cap != null && cap.get("costInitial") != null ? ((Number) cap.get("costInitial")).doubleValue() : 0.0;
            double costFinal   = cap != null && cap.get("costFinal")   != null ? ((Number) cap.get("costFinal")).doubleValue()   : 0.0;
            drone.put("costPer100Moves", costInitial + costFinal + costPerMove * 100);
        });

        return drones;
    }

    private S3Client getS3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Configuration.getS3Endpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                systemEnvironment.getAwsUser(),
                                systemEnvironment.getAwsSecret())))
                .region(systemEnvironment.getAwsRegion())
                .build();
    }
}