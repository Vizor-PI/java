package com.sptech.school;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class Etl {

    private final String baseOutputDir;
    private final String jdbcUrl;
    private final String dbUser;
    private final String dbPass;

    // Construtor que aceita base output (ex: "/tmp" no Lambda)
    public Etl(String baseOutputDir, String jdbcUrl, String dbUser, String dbPass) {
        this.baseOutputDir = baseOutputDir;
        this.jdbcUrl = jdbcUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
    }

    public void processarCSV(String caminhoEntrada) {

        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass)) {
            System.out.println("Conectado ao banco");

            int cpuMax = 85, memMax = 85, discoMax = 85, tempMax = 75;
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT c.nome, p.valorParametro
                    FROM parametro p
                    JOIN componente c ON p.fkComponente = c.id;
                """);
                while (rs.next()) {
                    String nome = rs.getString("nome").toUpperCase();
                    int valor = rs.getInt("valorParametro");
                    if (nome.equals("CPU")) cpuMax = valor;
                    else if (nome.equals("RAM")) memMax = valor;
                    else if (nome.equals("DISCO")) discoMax = valor;
                    else if (nome.contains("TEMP")) tempMax = valor;
                }
            }

            System.out.printf("Parametros: CPU=%d, RAM=%d, DISCO=%d, TEMP=%d%n", cpuMax, memMax, discoMax, tempMax);

            File arquivo = new File(caminhoEntrada);
            if (!arquivo.exists()) {
                System.out.println("Arquivo nao encontrado: " + arquivo.getAbsolutePath());
                return;
            }

            BufferedReader leitor = new BufferedReader(new FileReader(arquivo));
            String cabecalho = leitor.readLine();
            if (cabecalho == null) {
                System.out.println("CSV vazio");
                leitor.close();
                return;
            }

            // Formatador para data BR
            SimpleDateFormat sdfInput = new SimpleDateFormat("dd/MM/yyyy");

            String linha;
            while ((linha = leitor.readLine()) != null) {
                // ignora linhas vazias ou com espacos
                linha = linha.trim();
                if (linha.isEmpty()) continue;

                System.out.println("Linha lida: " + linha);

                // Campos do raw sao separados por ;
                String[] campos = linha.split(";");

                // RAW com 11 colunas
                if (campos.length < 10) {
                    System.out.println("Linha invalida (tamanho): " + linha);
                    continue;
                }

                // Campos que ja tem no CSV
                String userId = campos[0];
                String timestamp = campos[1];

                // Fomatação completa das colunas do CSV
                // Replace de vírgula por ponto para conversão Double
                double cpu = Double.parseDouble(campos[2].replace(",", "."));
                double mem = Double.parseDouble(campos[3].replace(",", "."));
                double disco = Double.parseDouble(campos[4].replace(",", "."));
                String uptimeMin = campos[8];
                double temp = Double.parseDouble(campos[9].replace(",", "."));
                String indoor = campos.length > 10 ? campos[10] : "0";

                String dataString = timestamp.split(" ")[0];
                // Tratamento de Data para evitar Duplicidade no Banco
                java.sql.Date dataSql;
                try {
                    java.util.Date date = sdfInput.parse(dataString);
                    dataSql = new java.sql.Date(date.getTime());
                } catch (Exception e) {
                    System.out.println("Erro parse data: " + dataString);
                    dataSql = new java.sql.Date(System.currentTimeMillis()); // Fallback para hoje
                }

                // Campos que precisam ser preenchidos com a busca no BD
                String empresa = "ERRO";
                String lote = "";
                String modelo = "";
                String enderecoTexto = "";
                String latitude = "0.0";
                String longitude = "0.0";

                // Buscar dados adicionais no banco
                try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT e.nome AS empresa,
                           l.id AS lote,
                           m.nome AS modelo,
                           en.rua,
                           en.numero,
                           en.bairro,
                           z.zona,
                           c.nome AS cidade,
                           en.latitude,
                           en.longitude
                    FROM miniComputador mc
                    JOIN lote l ON mc.fkLote = l.id
                    JOIN empresa e ON l.fkEmpresa = e.id
                    JOIN modelo m ON l.fkModelo = m.id
                    JOIN endereco en ON mc.fkEndereco = en.id
                    JOIN zona z ON en.fkZona = z.id
                    JOIN cidade c ON en.fkCidade = c.id
                    WHERE mc.codigo = ?;
                """)) {

                    ps.setString(1, userId);
                    ResultSet rs = ps.executeQuery();

                    // Preenchendo os campos adicionais com o resultado da query
                    if (rs.next()) {
                        empresa = rs.getString("empresa");
                        lote = rs.getString("lote");
                        modelo = rs.getString("modelo");

                        // Usando rs.getString pois no SQL lat e long são DECIMAL
                        latitude = rs.getString("latitude");
                        longitude = rs.getString("longitude");

                        // Montando o endereco completo
                        enderecoTexto = rs.getString("rua") + ", "
                                + rs.getString("numero") + " - "
                                + rs.getString("bairro") + " ("
                                + rs.getString("zona") + ", "
                                + rs.getString("cidade") + ")";
                    }
                }

                // Situação do alerta e atribuindo situações que devem ser abertos chamados
                String situacao;
                boolean deveCriarChamado = false;

                // Criando a variavel de situacao
                if (cpu > cpuMax || mem > memMax || disco > discoMax || temp > tempMax) {
                    situacao = "Critico";
                    deveCriarChamado = true;
                } else if (cpu > 0.7 * cpuMax || mem > 0.7 * memMax || disco > 0.7 * discoMax || temp > 0.7 * tempMax) {
                    situacao = "Alerta";
                    deveCriarChamado = true;
                } else {
                    situacao = "Otimo";
                }

                // Verificar existência do chamado no banco
                boolean chamadoJaExiste = false;

                if (!situacao.equals("Otimo")) {
                    try (PreparedStatement psCheck = conn.prepareStatement("""
                        SELECT 1 FROM chamados_gerados
                        WHERE codigo_maquina = ?
                        AND data_alerta = ?
                        AND situacao = ?
                        LIMIT 1;
                    """)) {
                        psCheck.setString(1, userId);
                        psCheck.setDate(2, dataSql);
                        psCheck.setString(3, situacao);
                        ResultSet rs = psCheck.executeQuery();
                        chamadoJaExiste = rs.next();
                    }
                }

                // Abrir chamado se necessário
                if (!situacao.equals("Otimo") && deveCriarChamado && !chamadoJaExiste) {

                    JiraClient.criarChamado(
                            userId,
                            empresa,
                            modelo,
                            lote,
                            enderecoTexto,
                            indoor,
                            uptimeMin,
                            cpu,
                            mem,
                            disco,
                            temp,
                            situacao,
                            timestamp
                    );


                    // Registrar no SQL se já foi criado o chamado e qual o estado do alerta
                    try (PreparedStatement psInsert = conn.prepareStatement("""
                    INSERT INTO chamados_gerados (codigo_maquina, data_alerta, situacao)
                    VALUES (?, ?, ?);
                """)) {
                        psInsert.setString(1, userId);
                        psInsert.setDate(2, dataSql);
                        psInsert.setString(3, situacao);
                        psInsert.executeUpdate();
                        System.out.println("Registrado no MySQL que o chamado ja foi criado.");
                    }
                }

                // Usar baseOutputDir ao invés de caminho fixo

                // Limpeza de strings para evitar erros no Path
                String empresaClean = empresa.replaceAll("[^a-zA-Z0-9.-]", "_");
                String dataClean = dataString.replaceAll("/", "-");

                String pasta = Paths.get(baseOutputDir, "trusted", empresaClean, userId, dataClean).toString() + File.separator;
                Files.createDirectories(Paths.get(pasta));

                String saida = pasta + "captura_dados_dooh.csv";
                File arquivoSaida = new File(saida);
                boolean novo = !arquivoSaida.exists();

                try (BufferedWriter escritor = new BufferedWriter(new FileWriter(saida, true))) {
                    if (novo) {
                        // Cabeçalho trusted
                        escritor.write("User,Timestamp,CPU,Memoria,Disco,Uptime,Temperatura,Indoor,Situacao,Latitude,Longitude");
                        escritor.newLine();
                    }

                    // Linha final formatada do trusted
                    String linhaFinal = String.format(Locale.US, "%s,%s,%.2f,%.2f,%.2f,%s,%.2f,%s,%s,%s,%s",
                            userId, timestamp, cpu, mem, disco, uptimeMin, temp, indoor, situacao, latitude, longitude
                    );

                    escritor.write(linhaFinal);
                    escritor.newLine();
                }

                String json = "{"
                        + "\"user\":\"" + userId + "\","
                        + "\"timestamp\":\"" + timestamp + "\","
                        + "\"cpu\":" + cpu + ","
                        + "\"memoria\":" + mem + ","
                        + "\"disco\":" + disco + ","
                        + "\"temp\":" + temp + ","
                        + "\"latitude\":\"" + latitude + "\","
                        + "\"longitude\":\"" + longitude + "\","
                        + "\"uptime_min\":\"" + uptimeMin + "\","
                        + "\"indoor\":\"" + indoor + "\","
                        + "\"empresa\":\"" + empresa + "\","
                        + "\"lote\":\"" + lote + "\","
                        + "\"modelo\":\"" + modelo + "\","
                        + "\"endereco\":\"" + enderecoTexto + "\","
                        + "\"situacao\":\"" + situacao + "\""
                        + "}";

                System.out.println("JSON GERADO -> " + json);
                System.out.println("Gerado: " + saida);

            }

            leitor.close();
            System.out.println("ETL finalizada");
        } catch (Exception e) {
            System.out.println("Erro na ETL: " + e.getMessage());
            e.printStackTrace();
        }
    }
}