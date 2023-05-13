package com.googlecode.distributedscheduling;

import java.io.*;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;
import java.lang.Math;

import static java.lang.System.out;

/**
 * @author apurv verma
 * @version 0.0
 */

/*
 * This class generates an ETC(Expected Time Completion) matrix to be used in the simulation.
 * e[i,j] represents the Expected time of execute the ith task ( t[i] ) on the jth machine ( m[j])
 */
public class ETCGenerator {

    /*The number of machines in the Heterogenous Computing suite*/
    int m;

    /*The number of tasks in the Heterogenous Computing suite*/
    int n;

    /*The Expected Time to Complete matrix*/
    double[][] etc;

    /*Task Heterogeneity*/
    int T_t;

    /*Machine Heterogeneity*/
    int T_m;

    public ETCGenerator(int NUM_MACHINES, int NUM_TASKS, TaskHeterogeneity TASK_HETEROGENEITY,
                        MachineHeterogeneity MACHINE_HETEROGENEITY) {
        m = NUM_MACHINES;
        n = NUM_TASKS;
        etc = new double[n][m];/*e[i][j] represents the time taken to complete the ith task on the jth machine.*/
        T_t = TASK_HETEROGENEITY.getNumericValue();
        T_m = MACHINE_HETEROGENEITY.getNumericValue();
    }

    public ETCGenerator(int NUM_MACHINES, int NUM_TASKS) {
        m = NUM_MACHINES;
        n = NUM_TASKS;
        etc = new double[n][m];/*e[i][j] represents the time taken to complete the ith task on the jth machine.*/
    }

    public ETCGenerator ETCEngine(int NUM_MACHINES, int NUM_TASKS, TaskHeterogeneity TASK_HETEROGENEITY,
                                  MachineHeterogeneity MACHINE_HETEROGENEITY) {
        m = NUM_MACHINES;
        n = NUM_TASKS;
        etc = new double[n][m];/*e[i][j] represents the time taken to complete the ith task on the jth machine.*/
        T_t = TASK_HETEROGENEITY.getNumericValue();
        T_m = MACHINE_HETEROGENEITY.getNumericValue();
        return this;
    }

    public void generateETCRandom(String fileName){
        try{
            Random rt = new Random();
            Random rm = new Random();
            double q[] = new double[n];
            double r[] = new double[m];

//        System.out.println("TASK_HETEROGENEITY: "+ T_t);
//        System.out.println("MACHINE_HETEROGENEITY: "+ T_m);

            FileOutputStream fos = null;
            BufferedWriter bw = null;
            if(fileName != null){
                fos = new FileOutputStream(new File(fileName));
                bw = new BufferedWriter(new OutputStreamWriter(fos));
                bw.write(n + " " + m);
                bw.newLine();
            }

//            for (int i = 0 ; i < n ; i++) { //numero de tasks
//                double range = Math.random() * ((q[i]*1.2) - (q[i]*0.8)) + (q[i]*0.8);
//                int N_t = rt.nextInt(T_t) + 1; //valor entre 0 e T_t(TASK_HETEROGENEITY)
//                q[i] = N_t;
//            }
//            for (int i = 0 ; i < n ; i++) { //numero de tasks
//                for (int j = 0 ; j < m ; j++) { //numero de maquinas
//                    int N_m = rm.nextInt(T_m) + 1; //valor entre 0 e T_m(MACHINE_HETEROGENEITY)
//                    double range = Math.random() * ((q[i]*1.2) - (q[i]*0.8)) + (q[i]*0.8);
//                    etc[i][j] = (range * N_m) + q[i];
//
//                    if(bw != null){
//                        bw.write(etc[i][j]+"");
//                        bw.newLine();
//                    }
//                }
//            }

            for (int i = 0 ; i < n ; i++) { //numero de tasks
                double percentTask = (float)T_t/100;
//                out.println("percentTask:"+percentTask);
                double max = 1000*(1+percentTask);
                double min = 1000*(1-percentTask);
//                out.println("max:"+max);
//                out.println("min:"+min);
                double N_t = (Math.random() * (max - min)) + min;
                q[i] = N_t;
//                out.println("q[i]:"+q[i]);
            }

//            for (int i = 0 ; i < m ; i++) { //numero de tasks
//                double percentMachine = (float)T_m/100;
//                double max = 100*(1+percentMachine);
//                double min = 100*(1-percentMachine);
//                double N_m = (Math.random() * (max - min)) + min;
//                r[i] = N_m;
//            }

            for (int i = 0 ; i < n ; i++) { //numero de tasks
                for (int j = 0 ; j < m ; j++) { //numero de maquinas
                    double percentMachine = (float)T_m/100;
                    double max = 1000*(1+percentMachine);
                    double min = 1000*(1-percentMachine);
                    double N_m = (Math.random() * (max - min)) + min;
                    etc[i][j] = q[i] * N_m;
//                    out.println("etc:"+etc[i][j]);
                    if(bw != null){
                        bw.write(etc[i][j]+"");
                        bw.newLine();
                    }
                }
            }

            if(bw != null)
                bw.close();

        }catch (IOException err){
            err.printStackTrace();
        }


    }

    public void generateETCbyFile(String nameFile){
        try {

            System.out.println("Vou ler o arquivo então2");
            Scanner scanner = new Scanner(new FileReader(nameFile));
            String line = scanner.nextLine();

            int rows = Integer.parseInt(line.split(" ")[0]); //qtd de tarefas
            int cols = Integer.parseInt(line.split(" ")[1]); //qtd de máquinas
            etc = new double[rows][cols];
            for (int i = 0; i < rows; i++)
                for (int j = 0; j < cols; j++) {
                    double teste = Double.parseDouble(scanner.nextLine());
//                    int teste = (int) (Double.valueOf(scanner.nextLine())*1000);
//
//                    if(teste <= 0 )
//                        out.println("etc negativo = "+ teste);
                    etc[i][j] = teste;
//                    out.println("etc["+i+"]["+j+"]="+ etc[i][j]);
                    //porque está pegando só o valor inteiro?
                    //i = tarefas
                    //j = máquinas
                    //Cada linha do arquivo representa quanto tempo a máquina j leva para completar a task i
                }

            scanner.close();
            System.out.println("Fechei o arquivo");
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    public double[][] getETC(){
        return etc;
    }

    @Override
    public String toString() {
        String s = Arrays.deepToString(this.getETC());
        return s;
    }


//    public static void main(String... args) {
//        TaskHeterogeneity TH = null;
//        MachineHeterogeneity MH = null;
//        int[][] ee = new ETCGenerator(3, 10, TH.HIGH, MH.HIGH).getETC();
//        for (int i = 0; i < ee.length; i++) {
//            for (int j = 0; j < ee[i].length; j++) {
//                out.print(ee[i][j] + " ");
//            }
//            out.println();
//        }
//        out.println(Arrays.deepToString(ee));
//    }
}
