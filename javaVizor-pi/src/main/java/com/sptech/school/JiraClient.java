package com.sptech.school;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.io.OutputStream;

public class JiraClient {

    private static final String JIRA_URL = "https://vizor.atlassian.net";
    private static final String EMAIL = "lucas.aquino@sptech.school";
    private static final String API_TOKEN = "";

    public static void criarChamado(
            String maquinaId,
            String empresa,
            String modelo,
            String lote,
            String enderecoTexto,
            String indoor,
            String uptimeMin,
            double cpu,
            double mem,
            double disco,
            String situacao,
            String timestamp
    ) {
        try {
            // Endpoint correto para JIRA SERVICE MANAGEMENT
            URL url = new URL(JIRA_URL + "/rest/servicedeskapi/request");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            // Autenticação
            String auth = EMAIL + ":" + API_TOKEN;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);

            // Monta descrição com todos os campos vindo da ETL
            String description = String.format(
                    "Máquina: %s\nEmpresa: %s\nModelo: %s\nLote: %s\nEndereço: %s\nIndoor: %s\nUptime (min): %s\nCPU: %.1f%%\nRAM: %.1f%%\nDisco: %.1f%%\nSituação: %s\nTimestamp: %s",
                    maquinaId, empresa, modelo, lote, enderecoTexto, indoor, uptimeMin,
                    cpu, mem, disco, situacao, timestamp
            );

            // Quebras de linha para JSON
            description = description.replace("\n", "\\n").replace("\"", "\\\"");

            // JSON compatível com JSM
            String json = """
        {
          "serviceDeskId": "1",
          "requestTypeId": "1",
          "requestFieldValues": {
            "summary": "%s - Máquina %s",
            "description": "%s"
          }
        }
        """.formatted(
                    situacao.toUpperCase(),
                    maquinaId,
                    description
            );

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes());
            }

            int status = conn.getResponseCode();
            System.out.println("Status criação do chamado (Service Desk): " + status);

            // Ler resposta do Jira (sucesso ou erro) e retornar se o chamado foi criado
            InputStream is = (status >= 200 && status < 300) ? conn.getInputStream() : conn.getErrorStream();
            if (status == 201) {
                System.out.println("Chamado criado com sucesso no Jira Service Desk.");
            }
            else if (is != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) response.append(line);
                br.close();
                System.out.println("Resposta Jira: " + response.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Erro ao criar chamado no Jira Service Desk: " + e.getMessage());
        }
    }
}
