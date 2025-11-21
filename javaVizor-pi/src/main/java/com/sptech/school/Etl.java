package com.sptech.school;

import java.io.*;
import java.nio.file.*;
import java.sql.*;

public class Etl {





    public void processarCSV(String caminhoEntrada) {
        String url = "jdbc:mysql://localhost:3306/vizor";
        String user = "root";
        String pass = "mafalu09";

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            System.out.println("Conectado ao banco");

            int cpuMax = 85, memMax = 85, discoMax = 85;
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
                }
            }

            System.out.printf("Parametros: CPU=%d, RAM=%d, DISCO=%d%n", cpuMax, memMax, discoMax);

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

            String linha;
            while ((linha = leitor.readLine()) != null) {
                // ignora linhas vazias ou com espacos
                linha = linha.trim();
                if (linha.isEmpty()) continue;

                System.out.println("Linha lida: " + linha);


                String[] campos = linha.split(",");
                if (campos.length != 10) {
                    System.out.println("Linha invalida: " + linha);
                    continue;
                }


                // Campos que ja tem no CSV
                String userId = campos[0];
                String timestamp = campos[1];
                double cpu = Double.parseDouble(campos[2]);
                double mem = Double.parseDouble(campos[3]);
                double disco = Double.parseDouble(campos[4]);
                String data = timestamp.split(" ")[0];
                String uptimeMin = campos[8];
                String indoor = campos[9];


                // Campos que precisam ser preenchidos com a busca no BD
                String empresa = "ERRO";
                String lote = "";
                String modelo = "";
                String enderecoTexto = "";

                // Buscar dados adicionais no banco
                try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT e.nome AS empresa,
                           l.id AS lote,
                           m.nome AS modelo,
                           en.rua,
                           en.numero,
                           en.bairro,
                           z.zona,
                           c.nome AS cidade
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

                if (cpu > cpuMax || mem > memMax || disco > discoMax) {
                    situacao = "Critico";
                    deveCriarChamado = true;
                } else if (cpu > 0.7 * cpuMax || mem > 0.7 * memMax || disco > 0.7 * discoMax) {
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
                        psCheck.setDate(2, java.sql.Date.valueOf(data));
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
                            situacao,
                            timestamp
                    );


                    // Registrar no SQL se já foi criado o chamado e qual o estado do alerta
                    try (PreparedStatement psInsert = conn.prepareStatement("""
                    INSERT INTO chamados_gerados (codigo_maquina, data_alerta, situacao)
                    VALUES (?, ?, ?);
                """)) {
                        psInsert.setString(1, userId);
                        psInsert.setDate(2, java.sql.Date.valueOf(data));
                        psInsert.setString(3, situacao);
                        psInsert.executeUpdate();
                        System.out.println("Registrado no MySQL que o chamado ja foi criado.");
                    }
                }

                // Criar pastas e arquivo de saída para cada pasta e data
                String pasta = "data/trusted/" + empresa + "/" + userId + "/" + data + "/";
                Files.createDirectories(Paths.get(pasta));

                String saida = pasta + "captura_dados_dooh.csv";
                File arquivoSaida = new File(saida);
                boolean novo = !arquivoSaida.exists();

                try (BufferedWriter escritor = new BufferedWriter(new FileWriter(saida, true))) {
                    if (novo) {
                        escritor.write(cabecalho + ",Situacao");
                        escritor.newLine();
                    }
                    escritor.write(linha + "," + situacao);
                    escritor.newLine();
                }

                // Gerando JSON com os dados processados
                String json = "{"
                        + "\"user\":\"" + userId + "\","
                        + "\"timestamp\":\"" + timestamp + "\","
                        + "\"cpu\":" + cpu + ","
                        + "\"memoria\":" + mem + ","
                        + "\"disco\":" + disco + ","
                        + "\"uptime_min\":\"" + uptimeMin + "\","
                        + "\"indoor\":\"" + indoor + "\","
                        + "\"empresa\":\"" + empresa + "\","
                        + "\"lote\":\"" + lote + "\","
                        + "\"modelo\":\"" + modelo + "\","
                        + "\"endereco\":\"" + enderecoTexto + "\","
                        + "\"situacao\":\"" + situacao + "\""
                        + "}";

                System.out.println("JSON GERADO: " + json);
                System.out.println("Gerado: " + saida);

            }

            leitor.close();
            System.out.println("ETL finalizada");
        } catch (Exception e) {
            System.out.println("Erro na ETL: " + e.getMessage());
        }
    }
}
