package com.sptech.school;

import java.io.*;
import java.nio.file.*;
import java.sql.*;

public class Etl {

    public void processarCSV(String caminhoEntrada) {
        String url = "jdbc:mysql://localhost:3306/vizor";
        String user = "aluno";
        String pass = "Sptech#2024";

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
                if (campos.length < 10) {
                    System.out.println("Linha invalida: " + linha);
                    continue;
                }

                String userId = campos[0];
                String timestamp = campos[1];
                double cpu = Double.parseDouble(campos[2]);
                double mem = Double.parseDouble(campos[3]);
                double disco = Double.parseDouble(campos[4]);
                String data = timestamp.split(" ")[0];

                String empresa = "ERRO";
                try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT e.nome AS nomeEmpresa
                    FROM miniComputador m
                    JOIN lote l ON m.fkLote = l.id
                    JOIN empresa e ON l.fkEmpresa = e.id
                    WHERE m.codigo = ?;
                """)) {
                    ps.setString(1, userId);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) empresa = rs.getString("nomeEmpresa");
                }

                String situacao;
                if (cpu > cpuMax || mem > memMax || disco > discoMax) situacao = "Critico";
                else if (cpu > 0.6 * cpuMax || mem > 0.6 * memMax || disco > 0.6 * discoMax) situacao = "Regular";
                else situacao = "Otimo";

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

                System.out.println("Gerado: " + saida);
            }

            leitor.close();
            System.out.println("ETL finalizada");
        } catch (Exception e) {
            System.out.println("Erro na ETL: " + e.getMessage());
        }
    }
}
