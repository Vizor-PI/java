package com.sptech.school;

public class Main {
    public static void main(String[] args) {
        Etl etl = new Etl();
        etl.processarCSV("data/raw/captura_dados_dooh.csv");
    }
}
