package com.sptech.school;


public class Main {
    public static void main(String[] args) {
        Etl etl = new Etl();
        String caminhoEntrada = "data/raw/captura_dados_dooh.csv";

        etl.processarCSV(caminhoEntrada);
    }
}
