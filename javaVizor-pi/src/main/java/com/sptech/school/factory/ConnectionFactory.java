package com.sptech.school.factory;

import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectionFactory {

    private static final String URL = "jdbc:mysql://localhost:3306/vizor";
    private static final String USER = "aluno";
    private static final String PASS = "Sptech#2024";

    public static Connection getConnection() {
        Connection conexao = null;
        try {
            conexao = DriverManager.getConnection(URL, USER, PASS);
            System.out.println("Conectado ao banco com sucesso!");
        } catch (Exception e) {
            System.out.println("Erro ao conectar ao banco: " + e.getMessage());
        }
        return conexao;
    }
}
