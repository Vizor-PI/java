package com.sptech.school.dao;

import com.sptech.school.factory.ConnectionFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ParametrosDAO {

    private int cpuMax;
    private int memMax;
    private int discoMax;

    public void carregarParametros() {
        try (Connection conn = ConnectionFactory.getConnection()) {
            String sql = "SELECT c.nome, p.valorParametro " +
                    "FROM parametro p " +
                    "JOIN componente c ON p.fkComponente = c.id;";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                String nome = rs.getString("nome").toUpperCase();
                int valor = rs.getInt("valorParametro");

                if (nome.equals("CPU")) cpuMax = valor;
                else if (nome.equals("RAM")) memMax = valor;
                else if (nome.equals("DISCO")) discoMax = valor;
            }

            rs.close();
            stmt.close();
            System.out.println("Parâmetros carregados: CPU=" + cpuMax + ", RAM=" + memMax + ", DISCO=" + discoMax);

        } catch (Exception e) {
            System.out.println("Erro ao buscar parâmetros: " + e.getMessage());
        }
    }

    public int getCpuMax() { return cpuMax; }
    public int getMemMax() { return memMax; }
    public int getDiscoMax() { return discoMax; }
}
