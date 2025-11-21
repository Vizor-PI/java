package com.sptech.school;

public class Main {
    public static void main(String[] args) {

        Etl etl = new Etl();

        // Processa o CSV inteiro, gera pastas e cria chamados automaticamente quando necessário
        etl.processarCSV("data/raw/captura_dados_dooh.csv");

        System.out.println("Processamento concluído.");
            }
        }
