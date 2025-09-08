package com.sptech.school;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        Maquina maquina = new Maquina();

        //Data
        LocalDate dataAtual = LocalDate.now();

            // Formato padrão da hora
        LocalTime horaAtual = LocalTime.now();

        // Obtém a data e hora atual
        LocalDateTime dataHoraAtual = LocalDateTime.now();

        // Formatar a hora
        DateTimeFormatter formatadorHora = DateTimeFormatter.ofPattern("HH:mm:ss");
        String horaFormatada = horaAtual.format(formatadorHora);

        maquina.cpuUso.add(57.8);
        maquina.cpuUso.add(45.3);
        maquina.cpuUso.add(67.1);
        maquina.cpuUso.add(72.2);
        maquina.cpuUso.add(25.6);

        maquina.memoriaTotal.add(8);
        maquina.memoriaTotal.add(16);
        maquina.memoriaTotal.add(16);
        maquina.memoriaTotal.add(8);
        maquina.memoriaTotal.add(8);

        maquina.memoriaUsada.add(4.4);
        maquina.memoriaUsada.add(5.6);
        maquina.memoriaUsada.add(13.1);
        maquina.memoriaUsada.add(7.3);
        maquina.memoriaUsada.add(3.9);

        maquina.discoUsado.add(1.4);
        maquina.discoUsado.add(2.1);
        maquina.discoUsado.add(1.9);
        maquina.discoUsado.add(3.1);
        maquina.discoUsado.add(4.7);

        maquina.rede.add(4);
        maquina.rede.add(8);
        maquina.rede.add(16);
        maquina.rede.add(16);
        maquina.rede.add(32);


        for (int i = 0; i < 5; i++) {

            System.out.println('\n');
            System.out.println("===== Monitoramento do Totem (" + (i + 1) + ") =====");

                //Data e hora

                /*System.out.println("Data atual: " + dataAtual);
                System.out.println("Hora atual: " + horaFormatada);*/

                // CPU
                System.out.println("Uso da CPU: " + maquina.cpuUso.get(i) + "%");

                // Memória
                System.out.println("Memória Total: " + maquina.memoriaTotal.get(i) + "GB");
                System.out.println("Memória Usada: " + maquina.memoriaUsada.get(i) + "GB");
                //System.out.println("Memória Livre: " + maquina.memoriaTotal.get(i) - Arredondado);

                // Disco
                System.out.println("Disco usado: " + maquina.discoUsado.get(i) + "GB");

                // Rede
                System.out.println("Rede: " + maquina.rede.get(i) + "GB");

                System.out.println();

                Thread.sleep(10000);


        }
    }

}

// Alterações que vou fazer: colocar mais calculos
// a cada vez que o for rodar ele vai acrescentar +10% pra emitir um aviso no final
// vou usar vetor pras colocar os valores obtidos do for
// usar orientação a objetos com classes e extends: empresa, usuário, máquina...
// pegar media de memoria e de cpu
// ex: (1.16 * 100) / 56.7

