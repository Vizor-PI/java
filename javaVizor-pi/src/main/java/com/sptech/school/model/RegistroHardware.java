package com.sptech.school.model;

public class RegistroHardware {
    private String user;
    private String timestamp;
    private double cpu;
    private double memoria;
    private double disco;
    private double rede;
    private double processos;
    private String dataBoot;
    private int uptime;
    private int indoor;
    private String situacao;

    public String getUser() { return user; }
    public void setUser(String user) { this.user = user; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public double getCpu() { return cpu; }
    public void setCpu(double cpu) { this.cpu = cpu; }

    public double getMemoria() { return memoria; }
    public void setMemoria(double memoria) { this.memoria = memoria; }

    public double getDisco() { return disco; }
    public void setDisco(double disco) { this.disco = disco; }

    public double getRede() { return rede; }
    public void setRede(double rede) { this.rede = rede; }

    public double getProcessos() { return processos; }
    public void setProcessos(double processos) { this.processos = processos; }

    public String getDataBoot() { return dataBoot; }
    public void setDataBoot(String dataBoot) { this.dataBoot = dataBoot; }

    public int getUptime() { return uptime; }
    public void setUptime(int uptime) { this.uptime = uptime; }

    public int getIndoor() { return indoor; }
    public void setIndoor(int indoor) { this.indoor = indoor; }

    public String getSituacao() { return situacao; }
    public void setSituacao(String situacao) { this.situacao = situacao; }
}
