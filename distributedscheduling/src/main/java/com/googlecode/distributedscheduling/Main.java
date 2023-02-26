package com.googlecode.distributedscheduling;

import java.io.*;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

import static java.lang.System.out;

/**
 * @author apurv verma
 */
public class Main {

    public static void main(String... args) {
        boolean byFile = false;
        boolean alwaysGenerateETC = false;

        /*Specify the parameters here*/
        int NUM_MACHINES = 16;//256;
        int NUM_TASKS = 512;//8192;
        int no_of_simulations = 2000;

        double ARRIVAL_RATE = 19;
        int metaSetSize = 64;
        String fileName = null;
        String fileNamenew = null;
        String fileNameResults = null;

        Heuristic h = null;
        TaskHeterogeneity TH = null;
        MachineHeterogeneity MH = null;
        Heuristic HEURISTIC = null;
        TaskHeterogeneity th = TH.HIGH;
        MachineHeterogeneity mh = MH.HIGH;

        System.out.println("TASK_HETEROGENEITY: "+ th);
        System.out.println("MACHINE_HETEROGENEITY: "+ mh);

        Scanner scannerUser = new Scanner(System.in);
        System.out.println("Você quer pegar os dados de um arquivo? S/N");
        String saveFile = scannerUser.nextLine();
        if (saveFile.equalsIgnoreCase("S")) {
            byFile = true;
            System.out.println("Qual o nome do arquivo que você quer ler?");
                fileName = "./instancias/"+scannerUser.nextLine();
            System.out.println("Quantas simulações?");
            no_of_simulations = Integer.parseInt(scannerUser.nextLine());
            try {

                System.out.println("Vou ler o arquivo então2");
                Scanner scanner = new Scanner(new FileReader(fileName));
                String line = scanner.nextLine();

                NUM_TASKS = Integer.parseInt(line.split(" ")[0]); //qtd de tarefas
                NUM_MACHINES = Integer.parseInt(line.split(" ")[1]); //qtd de máquinas

                System.out.println("Numero de maquinas: "+ NUM_MACHINES);
                System.out.println("Numero de tarefas "+ NUM_TASKS);

                scanner.close();
                System.out.println("Fechei o arquivo");
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            }

        }
        else {
            System.out.println("Vou precisar de alguns dados:");
            System.out.println("Quantas máquinas?");
            NUM_MACHINES = Integer.parseInt(scannerUser.nextLine());

            System.out.println("Quantas tasks?");
            NUM_TASKS = Integer.parseInt(scannerUser.nextLine());

            System.out.println("Quantas simulações?");
            no_of_simulations = Integer.parseInt(scannerUser.nextLine());



            System.out.println("Você quer salvar os dados gerados em um arquivo para utilizar em todas as simulações? S/N");
            if (scannerUser.nextLine().equalsIgnoreCase("S")) {
                System.out.println("Qual o nome do arquivo?");
                fileNamenew = scannerUser.nextLine();
                File newFile = new File(fileNamenew);
                boolean result;
                try {
                    result = newFile.createNewFile();  //creates a new file
                    if (result)      // test if successfully created a new file
                    {
                        System.out.println("file created " + newFile.getCanonicalPath()); //returns the path string
                    } else {
                        System.out.println("File already exist at location: " + newFile.getCanonicalPath());
                    }
//                    fileScanner = new Scanner(new FileReader(fileName));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else{
                System.out.println("Você quer gerar um ETC novo para cada nova simulação?S/N");
                if (scannerUser.nextLine().equalsIgnoreCase("S"))
                    alwaysGenerateETC = true;
            }

        }

        System.out.println("Qual Nome do arquivo que você quer criar para colocar os resultados?");
        fileNameResults = "./resultados/"+scannerUser.nextLine();
        File resultados = new File(fileNameResults);


        long t1 = System.currentTimeMillis();


        /*Specify the parameters here*/

        Heuristic[] htype = Heuristic.values();
        long[] sigmaMakespan = new long[htype.length];
        double[] averageUtilizationList = new double[htype.length];
        long[] flowTimeList = new long[htype.length];
        long[] computationTimeList = new long[htype.length];
        Map<Integer, List<Long>> makespans = new HashMap<>();
        Map<Integer, List<Double>> avgUtilizations = new HashMap<>();
        Map<Integer, List<Long>> flowtimes = new HashMap<>();
        Map<Integer, List<Double>> computationTimes = new HashMap<>();
        long avgMakespan;
        double averageUtilization;
        double averageFlowTime;
        double averageComputationTime;

        SimulatorEngine se;
        if (fileName != null)
            se = new SimulatorEngine(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, null, fileName);
        else if (fileNamenew != null)
            se = new SimulatorEngine(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, null, th, mh, fileNamenew);
        else se = new SimulatorEngine(NUM_MACHINES, NUM_TASKS, ARRIVAL_RATE, metaSetSize, null, th, mh);

        for (int i = 0; i < no_of_simulations; i++) {

            se.newSimulation(alwaysGenerateETC, fileName);
//            se.cleanMatPriorityQueue();

            for (int j = 0; j < htype.length; j++) {
                se.setHeuristic(htype[j]);
                se.simulate();

                sigmaMakespan[j] += se.getMakespan();
                makespans.putIfAbsent(j, new ArrayList<>());
                List<Long> makespansForHeuristic = makespans.get(j);
                makespansForHeuristic.add(se.getMakespan());
                makespans.replace(j, makespansForHeuristic);

//                out.println("FlowTime se.mat.sum:"+ Arrays.stream(se.mat).sum());
//                out.println("SumMat calculado separado:" + se.sumMAT());
                averageUtilizationList[j] += ((double) se.sumMAT() / (double) (se.makespan * se.m));
                avgUtilizations.putIfAbsent(j, new ArrayList<>());
                List<Double> avgUtilizationsForHeuristic = avgUtilizations.get(j);
                avgUtilizationsForHeuristic.add((double) se.sumMAT() / (double) (se.makespan * se.m));
                avgUtilizations.replace(j, avgUtilizationsForHeuristic);


                flowTimeList[j] += se.sumMAT();
                flowtimes.putIfAbsent(j, new ArrayList<>());
                List<Long> avgFlowTimesForHeuristic = flowtimes.get(j);
                avgFlowTimesForHeuristic.add((long) se.sumMAT());
                flowtimes.replace(j, avgFlowTimesForHeuristic);

                computationTimeList[j] += se.eng.computationTime;
//                System.out.println(j + " ComputationalTime: " + se.eng.computationTime);
                computationTimes.putIfAbsent(j, new ArrayList<>());
                List<Double> computationTimesForHeuristic = computationTimes.get(j);
                computationTimesForHeuristic.add((double) se.eng.computationTime);
                computationTimes.replace(j, computationTimesForHeuristic);

                se.newSimulation(false, fileName);
                se.cleanMatPriorityQueue();

                //Antes chamava essa função, que não gerava uma nova simulação, só limpava o mat e a fila de prioridade: se.newSimulation(false);
            }
        }

        List<Long> avgMakespans = new ArrayList<>();
        List<Double> makespanStandardDeviations = new ArrayList<>();
        List<Double> avgUtilizationList = new ArrayList<>();
        List<Double> utilizationStandardDeviations = new ArrayList<>();
        List<Double> flowTimes = new ArrayList<>();
        List<Double> flowtimeStandardDeviations = new ArrayList<>();
        List<Double> computationTimesList = new ArrayList<>();
        List<Double> computationStandardDeviations = new ArrayList<>();


        BufferedWriter bw = null;
        FileOutputStream fos = null;
        try{
            fos = new FileOutputStream(resultados);
            bw = new BufferedWriter(new OutputStreamWriter(fos));

            for (int j = 0; j < htype.length; j++) {

                avgMakespan = sigmaMakespan[j] / no_of_simulations;
                avgMakespans.add(avgMakespan);
                Double standardDeviation = calculateStandardDeviation(makespans.get(j), avgMakespan);
                makespanStandardDeviations.add(standardDeviation);

                averageUtilization = averageUtilizationList[j] / no_of_simulations;
                avgUtilizationList.add(averageUtilization);
                Double utilizationStandardDeviation = calculateStandardDeviation(avgUtilizations.get(j), averageUtilization);
                utilizationStandardDeviations.add(utilizationStandardDeviation);

                averageFlowTime = (double) flowTimeList[j] / no_of_simulations;
                flowTimes.add(averageFlowTime);
                Double flowtimeStandardDeviation = calculateStandardDeviation(flowtimes.get(j), (long) averageFlowTime);
                flowtimeStandardDeviations.add(flowtimeStandardDeviation);

                averageComputationTime = (double) computationTimeList[j] / no_of_simulations;
                computationTimesList.add(averageComputationTime);
                Double computationStandardDeviation = calculateStandardDeviation(computationTimes.get(j), averageComputationTime);
                computationStandardDeviations.add(computationStandardDeviation);


                String hName = htype[j].toString();

                bw.write("Heuristic Name:"+ hName);
                bw.newLine();

                bw.write("Average Makespan:"+ avgMakespan);
                bw.newLine();
                bw.write("Standard Deviation makespan:"+ standardDeviation);
                bw.newLine();

                bw.write("Average Utilization:"+ standardDeviation);
                bw.newLine();
                bw.write("Standard Deviation Average Utilization:"+ utilizationStandardDeviation);
                bw.newLine();

                bw.write("Average flowtime:"+ averageFlowTime);
                bw.newLine();
                bw.write("Standard Deviation Average flowtime:"+ flowtimeStandardDeviation);
                bw.newLine();

                bw.write("Average Computation Time:"+ averageComputationTime);
                bw.newLine();
                bw.write("Standard Deviation Average Computation Time:"+ computationStandardDeviation);
                bw.newLine();
                bw.newLine();


                String outputAvg = String.format("%12.8E", (double) avgMakespan);
                String outputDp = String.format("%12.8E", standardDeviation);

                String outputAvgUt = String.format("%8.8f", averageUtilization);
                String outputAvgUtDp = String.format("%8.8f", utilizationStandardDeviation);

                String outputAvgFlowTime = String.format("%12.8E", averageFlowTime);
                String outputAvgFlowTimeDp = String.format("%12.8E", flowtimeStandardDeviation);


                out.println("\nAvg makespan for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + avgMakespan);
                out.println("Makespan Standard Deviation for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + standardDeviation);

                out.println("Average Utilization for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + averageUtilization);
                out.println("Average Utilization Standard Deviation for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + utilizationStandardDeviation);

                out.println("Average Flowtime for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + averageFlowTime);
                out.println("Flowtime Standard Deviation for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + flowtimeStandardDeviation);

                out.println("Average Computation Time for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + averageComputationTime + "ms.");
                out.println("Computation Time Standard Deviation for " + hName + " heuristic for " + no_of_simulations + " simulations is =  " + computationStandardDeviation + "ms.");
            }
            if(bw != null)
                bw.close();
        }catch (IOException err){
            err.printStackTrace();
        }

        long t2 = System.currentTimeMillis();
        out.println("\nTotal time taken in the simulation = " + (t2 - t1) / 1000 + " sec.");
        out.println("Minimum avg makespan = " + htype[avgMakespans.indexOf(Collections.min(avgMakespans))].toString());
        out.println("Maximum avg makespan = " + htype[avgMakespans.indexOf(Collections.max(avgMakespans))].toString());
        out.println("Minimum makespan standard deviation = " + htype[makespanStandardDeviations.indexOf(Collections.min(makespanStandardDeviations))].toString());
        out.println("Maximum makespan standard deviation = " + htype[makespanStandardDeviations.indexOf(Collections.max(makespanStandardDeviations))].toString());

        out.println("Minimum avg utilization = " + htype[avgUtilizationList.indexOf(Collections.min(avgUtilizationList))].toString());
        out.println("Maximum avg utilization = " + htype[avgUtilizationList.indexOf(Collections.max(avgUtilizationList))].toString());

        out.println("Minimum flowtime = " + htype[flowTimes.indexOf(Collections.min(flowTimes))].toString());
        out.println("Maximum flowtime = " + htype[flowTimes.indexOf(Collections.max(flowTimes))].toString());

        out.println("Minimum Computation Time = "
                + htype[computationTimesList.indexOf(Collections.min(computationTimesList))].toString());

        out.println("Maximum Computation Time = "
                + htype[computationTimesList.indexOf(Collections.max(computationTimesList))].toString());

    }


    private static Double calculateStandardDeviation(List<Long> makespans, long avgMakespan) {

        long differenceSum =
                makespans.stream().mapToLong(makespan -> (long) Math.pow(Math.abs(makespan - avgMakespan), 2)).sum();
        long division = differenceSum / makespans.size();
        return Math.sqrt(division);
    }

    private static Double calculateStandardDeviation(List<Double> makespans, double avgMakespan) {

        double differenceSum =
                makespans.stream().mapToDouble(makespan -> Math.pow(Math.abs(makespan - avgMakespan), 2)).sum();
        double division = differenceSum / makespans.size();
        return Math.sqrt(division);
    }
}
