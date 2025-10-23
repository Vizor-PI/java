package com.sptech.school.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MiniComputadorDAO {

    private Connection conn;

    public MiniComputadorDAO(Connection conn) {
        this.conn = conn;
    }

    public String buscarEmpresaPorCodigo(String codigoMiniPc) {
        String nomeEmpresa = "Desconhecida";

        try {
            String sql = """
                SELECT e.nome AS nomeEmpresa
                FROM miniComputador m
                JOIN lote l ON m.fkLote = l.id
                JOIN empresa e ON l.fkEmpresa = e.id
                WHERE m.codigo = ?;
            """;

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, codigoMiniPc);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                nomeEmpresa = rs.getString("nomeEmpresa");
            }

            rs.close();
            ps.close();
        } catch (Exception e) {
            System.out.println("Erro ao buscar empresa: " + e.getMessage());
        }

        return nomeEmpresa;
    }
}
