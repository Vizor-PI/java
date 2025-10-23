package com.sptech.school;

import com.sptech.school.dao.ParametrosDAO;
import com.sptech.school.dao.MiniComputadorDAO;
import com.sptech.school.model.RegistroHardware;
import com.sptech.school.factory.ConnectionFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.*;

public class Etl {

    public void processarCSV(String caminhoEntrada) {
        try (Connection conn = ConnectionFactory.getConnection()) {

            if (conn == null) {
                System.out.println("Erro: conexão não estabelecida.");
                return;
            }

            ParametrosDAO parametroDAO = new ParametrosDAO();
            parametroDAO.carregarParametros();

            MiniComputadorDAO miniComputadorDAO = new MiniComputadorDAO(conn);

            File arquivo = new File(caminhoEntrada);
            if (!arquivo.exists()) {
                System.out.println("Erro: arquivo não encontrado -> " + arquivo.getAbsolutePath());
                return;
            }

            try (BufferedReader leitor = new BufferedReader(new FileReader(arquivo))) {
                String cabecalho = leitor.readLine();
                if (cabecalho == null) {
                    System.out.println("Erro: CSV vazio.");
                    return;
                }

                // Mapa para agrupar por empresa + máquina + data
                Map<String, List<String>> grupos = new HashMap<>();

                String linha;
                while ((linha = leitor.readLine()) != null) {
                    String[] campos = linha.split(",");

                    RegistroHardware r = new RegistroHardware();
                    r.setUser(campos[0]);
                    r.setTimestamp(campos[1]);
                    r.setCpu(Double.parseDouble(campos[2]));
                    r.setMemoria(Double.parseDouble(campos[3]));
                    r.setDisco(Double.parseDouble(campos[4]));
                    r.setRede(Double.parseDouble(campos[5]));
                    r.setProcessos(Double.parseDouble(campos[6]));
                    r.setDataBoot(campos[7]);
                    r.setUptime((int) Double.parseDouble(campos[8]));
                    r.setIndoor((int) Double.parseDouble(campos[9]));

                    String situacao = definirSituacao(r, parametroDAO);

                    // Obtém empresa e data
                    String empresa = miniComputadorDAO.buscarEmpresaPorCodigo(r.getUser());
                    String data = r.getTimestamp().split(" ")[0];

                    // Cria uma chave única por empresa/máquina/data
                    String chave = empresa + "|" + r.getUser() + "|" + data;

                    // Adiciona a linha formatada ao grupo correspondente
                    grupos.computeIfAbsent(chave, k -> new ArrayList<>()).add(linha + "," + situacao);
                }

                // Agora escreve um arquivo para cada grupo
                for (String chave : grupos.keySet()) {
                    String[] partes = chave.split("\\|");
                    String empresa = partes[0];
                    String maquina = partes[1];
                    String data = partes[2];

                    String pastaDestino = "data/trusted/" + empresa + "/" + maquina + "/" + data + "/";
                    Files.createDirectories(Paths.get(pastaDestino));

                    String caminhoSaida = pastaDestino + "captura_dados_dooh.csv";

                    try (BufferedWriter escritor = new BufferedWriter(new FileWriter(caminhoSaida))) {
                        escritor.write(cabecalho + ",Situacao");
                        escritor.newLine();
                        for (String l : grupos.get(chave)) {
                            escritor.write(l);
                            escritor.newLine();
                        }
                    }

                    System.out.println("Gerado: " + caminhoSaida);
                }

                System.out.println("✅ ETL finalizada com sucesso!");
            }

        } catch (Exception e) {
            System.out.println("Erro na ETL: " + e.getMessage());
        }
    }

    private String definirSituacao(RegistroHardware r, ParametrosDAO dao) {
        double cpuMax = dao.getCpuMax();
        double memMax = dao.getMemMax();
        double discoMax = dao.getDiscoMax();

        if (r.getCpu() > cpuMax || r.getMemoria() > memMax || r.getDisco() > discoMax) {
            return "Crítico";
        } else if (r.getCpu() > 0.6 * cpuMax || r.getMemoria() > 0.6 * memMax || r.getDisco() > 0.6 * discoMax) {
            return "Regular";
        } else {
            return "Ótimo";
        }
    }
}
