package com.sptech.school;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.file.*;
import java.util.stream.Stream;

public class LambdaHandler implements RequestHandler<S3Event, String> {

    private final String trustedBucket = System.getenv("TRUSTED_BUCKET"); // vizor-trusted
    // DB envs
    private final String dbHost = System.getenv("DB_HOST"); // IP da EC2
    private final String dbPort = System.getenv().getOrDefault("DB_PORT", "3306");
    private final String dbName = System.getenv().getOrDefault("DB_NAME", "vizor");
    private final String dbUser = System.getenv("DB_USER");
    private final String dbPass = System.getenv("DB_PASS");

    private final S3Client s3;

    public LambdaHandler() {
        s3 = S3Client.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
    }

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        try {
            // pega primeiro registro do evento
            var record = s3event.getRecords().get(0);
            String bucket = record.getS3().getBucket().getName();
            String key = record.getS3().getObject().getKey();

            System.out.println("Trigger recebido do bucket: " + bucket + " key: " + key);

            // baixa o objeto para /tmp/input.csv (usar nome único por execução)
            Path tmpInput = Paths.get("/tmp", "input.csv");
            Files.deleteIfExists(tmpInput);

            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            try (InputStream in = s3.getObject(getReq)) {
                Files.copy(in, tmpInput);
            }

            System.out.println("Arquivo baixado para: " + tmpInput.toString());

            // montar jdbc url para EC2
            String jdbcUrl = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;

            // instanciar ETL com baseOutputDir = /tmp
            Etl etl = new Etl("/tmp", jdbcUrl, dbUser, dbPass);
            etl.processarCSV(tmpInput.toString());

            // após processar, fazer upload de todos os arquivos em /tmp/trusted para S3
            Path trustedRoot = Paths.get("/tmp", "trusted");
            if (Files.exists(trustedRoot)) {
                try (Stream<Path> paths = Files.walk(trustedRoot)) {
                    paths.filter(Files::isRegularFile).forEach(filePath -> {
                        try {
                            // obtém key relativo: trustedRoot.relativize(filePath) => empresa/COD/YYYY-MM-...
                            Path relative = trustedRoot.relativize(filePath);
                            String s3Key = relative.toString().replace(File.separatorChar, '/'); // empresa/COD/date/filename
                            PutObjectRequest putReq = PutObjectRequest.builder()
                                    .bucket(trustedBucket)
                                    .key(s3Key)
                                    .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
                                    .build();
                            s3.putObject(putReq, RequestBody.fromFile(filePath));
                            System.out.println("Arquivo enviado para S3: " + trustedBucket + "/" + s3Key);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
            } else {
                System.out.println("Nenhum arquivo gerado em /tmp/trusted");
            }

            return "Processamento OK";

        } catch (Exception e) {
            e.printStackTrace();
            return "Erro: " + e.getMessage();
        }
    }
}
